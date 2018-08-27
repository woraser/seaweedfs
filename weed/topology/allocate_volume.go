package topology

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/util"
)

/*
//分配volume卷
*/

//分配volume卷的异常
type AllocateVolumeResult struct {
	Error string
}

//分配volume卷
func AllocateVolume(dn *DataNode, vid storage.VolumeId, option *VolumeGrowOption) error {
	//values := make(map[string][]string)
	values := make(url.Values)
	values.Add("volume", vid.String())
	values.Add("collection", option.Collection)
	values.Add("replication", option.ReplicaPlacement.String())
	values.Add("ttl", option.Ttl.String())
	values.Add("preallocate", fmt.Sprintf("%d", option.Prealloacte))
	jsonBlob, err := util.Post("http://"+dn.Url()+"/admin/assign_volume", values)
	if err != nil {
		return err
	}
	var ret AllocateVolumeResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return fmt.Errorf("Invalid JSON result for %s: %s", "/admin/assign_volum", string(jsonBlob))
	}
	if ret.Error != "" {
		return errors.New(ret.Error)
	}
	return nil
}
