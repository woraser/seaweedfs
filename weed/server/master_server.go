package weed_server

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"

	"github.com/chrislusf/raft"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/sequence"
	"github.com/chrislusf/seaweedfs/weed/topology"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/gorilla/mux"
)

type MasterServer struct {
	//对外端口号
	port                    int
	//文件存储目录
	metaFolder              string
	//volume卷大小限制
	volumeSizeLimitMB       uint
	//预分配大小
	preallocate             int64
	//心跳发送周期
	pulseSeconds            int
	//默认的备份策略
	defaultReplicaPlacement string
	//垃圾处理的阀值
	garbageThreshold        string
	guard                   *security.Guard

	Topo   *topology.Topology
	vg     *topology.VolumeGrowth
	vgLock sync.Mutex

	bounedLeaderChan chan int

	// notifying clients 用于通知客户端
	//客户端读写锁通道
	clientChansLock sync.RWMutex
	clientChans     map[string]chan *master_pb.VolumeLocation
}

//创建新的领导者节点
func NewMasterServer(r *mux.Router, port int, metaFolder string,
	volumeSizeLimitMB uint,
	preallocate bool,
	pulseSeconds int,
	defaultReplicaPlacement string,
	garbageThreshold string,
	whiteList []string,
	secureKey string,
) *MasterServer {

	var preallocateSize int64
	if preallocate {
		preallocateSize = int64(volumeSizeLimitMB) * (1 << 20)
	}
	ms := &MasterServer{
		port:                    port,
		volumeSizeLimitMB:       volumeSizeLimitMB,
		preallocate:             preallocateSize,
		pulseSeconds:            pulseSeconds,
		defaultReplicaPlacement: defaultReplicaPlacement,
		garbageThreshold:        garbageThreshold,
		clientChans:             make(map[string]chan *master_pb.VolumeLocation),
	}
	ms.bounedLeaderChan = make(chan int, 16)
	seq := sequence.NewMemorySequencer()
	ms.Topo = topology.NewTopology("topo", seq, uint64(volumeSizeLimitMB)*1024*1024, pulseSeconds)
	ms.vg = topology.NewDefaultVolumeGrowth()
	glog.V(0).Infoln("Volume Size Limit is", volumeSizeLimitMB, "MB")

	ms.guard = security.NewGuard(whiteList, secureKey)

	r.HandleFunc("/", ms.uiStatusHandler)
	r.HandleFunc("/ui/index.html", ms.uiStatusHandler)
	r.HandleFunc("/dir/assign", ms.proxyToLeader(ms.guard.WhiteList(ms.dirAssignHandler)))
	r.HandleFunc("/dir/lookup", ms.proxyToLeader(ms.guard.WhiteList(ms.dirLookupHandler)))
	r.HandleFunc("/dir/status", ms.proxyToLeader(ms.guard.WhiteList(ms.dirStatusHandler)))
	r.HandleFunc("/col/delete", ms.proxyToLeader(ms.guard.WhiteList(ms.collectionDeleteHandler)))
	r.HandleFunc("/vol/lookup", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeLookupHandler)))
	r.HandleFunc("/vol/grow", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeGrowHandler)))
	r.HandleFunc("/vol/status", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeStatusHandler)))
	r.HandleFunc("/vol/vacuum", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeVacuumHandler)))
	r.HandleFunc("/submit", ms.guard.WhiteList(ms.submitFromMasterServerHandler))
	r.HandleFunc("/delete", ms.guard.WhiteList(ms.deleteFromMasterServerHandler))
	r.HandleFunc("/stats/health", ms.guard.WhiteList(statsHealthHandler))
	r.HandleFunc("/stats/counter", ms.guard.WhiteList(statsCounterHandler))
	r.HandleFunc("/stats/memory", ms.guard.WhiteList(statsMemoryHandler))
	r.HandleFunc("/{fileId}", ms.proxyToLeader(ms.redirectHandler))

	ms.Topo.StartRefreshWritableVolumes(garbageThreshold, ms.preallocate)

	return ms
}

func (ms *MasterServer) SetRaftServer(raftServer *RaftServer) {
	ms.Topo.RaftServer = raftServer.raftServer
	ms.Topo.RaftServer.AddEventListener(raft.LeaderChangeEventType, func(e raft.Event) {
		glog.V(0).Infof("event: %+v", e)
		if ms.Topo.RaftServer.Leader() != "" {
			glog.V(0).Infoln("[", ms.Topo.RaftServer.Name(), "]", ms.Topo.RaftServer.Leader(), "becomes leader.")
		}
	})
	if ms.Topo.IsLeader() {
		glog.V(0).Infoln("[", ms.Topo.RaftServer.Name(), "]", "I am the leader!")
	} else {
		if ms.Topo.RaftServer.Leader() != "" {
			glog.V(0).Infoln("[", ms.Topo.RaftServer.Name(), "]", ms.Topo.RaftServer.Leader(), "is the leader.")
		}
	}
}

func (ms *MasterServer) proxyToLeader(f func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if ms.Topo.IsLeader() {
			f(w, r)
		} else if ms.Topo.RaftServer != nil && ms.Topo.RaftServer.Leader() != "" {
			ms.bounedLeaderChan <- 1
			defer func() { <-ms.bounedLeaderChan }()
			targetUrl, err := url.Parse("http://" + ms.Topo.RaftServer.Leader())
			if err != nil {
				writeJsonError(w, r, http.StatusInternalServerError,
					fmt.Errorf("Leader URL http://%s Parse Error: %v", ms.Topo.RaftServer.Leader(), err))
				return
			}
			glog.V(4).Infoln("proxying to leader", ms.Topo.RaftServer.Leader())
			proxy := httputil.NewSingleHostReverseProxy(targetUrl)
			director := proxy.Director
			proxy.Director = func(req *http.Request) {
				actualHost, err := security.GetActualRemoteHost(req)
				if err == nil {
					req.Header.Set("HTTP_X_FORWARDED_FOR", actualHost)
				}
				director(req)
			}
			proxy.Transport = util.Transport
			proxy.ServeHTTP(w, r)
		} else {
			//drop it to the floor
			//writeJsonError(w, r, errors.New(ms.Topo.RaftServer.Name()+" does not know Leader yet:"+ms.Topo.RaftServer.Leader()))
		}
	}
}
