package predicates

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	v1 "github.com/mongodb/mongodb-kubernetes/api/mongodb/v1"
	mdbv1 "github.com/mongodb/mongodb-kubernetes/mongodb-community-operator/api/v1"
)

func TestOnlyOnSpecChange(t *testing.T) {
	for _, tc := range []struct {
		name            string
		alreadyDeleting bool
		update          func(*mdbv1.MongoDBCommunity)
		want            bool
	}{
		{
			name:   "annotation updates do not reconcile",
			update: func(mdb *mdbv1.MongoDBCommunity) { mdb.Annotations = map[string]string{"example.com/test": "value"} },
		},
		{
			name:   "finalizer updates do not reconcile",
			update: func(mdb *mdbv1.MongoDBCommunity) { mdb.Finalizers = []string{"example.com/cleanup"} },
		},
		{
			name:   "status updates do not reconcile",
			update: func(mdb *mdbv1.MongoDBCommunity) { mdb.Status.Message = "ready" },
		},
		{
			name:   "spec updates reconcile",
			update: func(mdb *mdbv1.MongoDBCommunity) { mdb.Spec.Members = 4 },
			want:   true,
		},
		{
			name: "starting deletion reconciles without a spec change",
			update: func(mdb *mdbv1.MongoDBCommunity) {
				now := metav1.Now()
				mdb.DeletionTimestamp = &now
			},
			want: true,
		},
		{
			name:            "annotation updates during deletion do not loop",
			alreadyDeleting: true,
			update:          func(mdb *mdbv1.MongoDBCommunity) { mdb.Annotations = map[string]string{"example.com/test": "value"} },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oldResource := &mdbv1.MongoDBCommunity{
				ObjectMeta: metav1.ObjectMeta{Name: "mongodb", Namespace: "mongodb"},
				Spec: mdbv1.MongoDBCommunitySpec{
					Type:                             mdbv1.ReplicaSet,
					Members:                          3,
					AdditionalMongodConfig:           v1.MongodConfiguration{MapWrapper: v1.NewMapWrapper()},
					AdditionalConnectionStringConfig: v1.NewMapWrapper(),
				},
			}
			if tc.alreadyDeleting {
				now := metav1.Now()
				oldResource.DeletionTimestamp = &now
			}
			newResource := oldResource.DeepCopy()
			if !reflect.DeepEqual(oldResource.Spec, newResource.Spec) {
				t.Fatal("test fixture must preserve the spec when copied")
			}
			tc.update(newResource)

			got := OnlyOnSpecChange().Update(event.UpdateEvent{ObjectOld: oldResource, ObjectNew: newResource})
			if got != tc.want {
				t.Fatalf("expected reconciliation=%v, got %v", tc.want, got)
			}
		})
	}
}
