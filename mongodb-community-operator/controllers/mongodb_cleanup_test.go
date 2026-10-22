package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "github.com/mongodb/mongodb-kubernetes/api/mongodb/v1"
	mdbv1 "github.com/mongodb/mongodb-kubernetes/mongodb-community-operator/api/v1"
	kubeClient "github.com/mongodb/mongodb-kubernetes/pkg/kube/client"
)

func TestReplicaSetReconcilerCleanupScramSecrets(t *testing.T) {
	lastApplied := newScramReplicaSet(mdbv1.MongoDBUser{
		Name: "testUser",
		PasswordSecretRef: v1.SecretKeyReference{
			Name: "password-secret-name",
		},
		ScramCredentialsSecretName: "scram-credentials",
	})

	t.Run("no change same resource", func(t *testing.T) {
		actual := getScramSecretsToDelete(lastApplied.Spec, lastApplied.Spec)

		assert.Equal(t, []string(nil), actual)
	})

	t.Run("new user new secret", func(t *testing.T) {
		current := newScramReplicaSet(
			mdbv1.MongoDBUser{
				Name: "testUser",
				PasswordSecretRef: v1.SecretKeyReference{
					Name: "password-secret-name",
				},
				ScramCredentialsSecretName: "scram-credentials",
			},
			mdbv1.MongoDBUser{
				Name: "newUser",
				PasswordSecretRef: v1.SecretKeyReference{
					Name: "password-secret-name",
				},
				ScramCredentialsSecretName: "scram-credentials-2",
			},
		)

		actual := getScramSecretsToDelete(current.Spec, lastApplied.Spec)

		assert.Equal(t, []string(nil), actual)
	})

	t.Run("old user new secret", func(t *testing.T) {
		current := newScramReplicaSet(mdbv1.MongoDBUser{
			Name: "testUser",
			PasswordSecretRef: v1.SecretKeyReference{
				Name: "password-secret-name",
			},
			ScramCredentialsSecretName: "scram-credentials-2",
		})

		expected := []string{"scram-credentials-scram-credentials"}
		actual := getScramSecretsToDelete(current.Spec, lastApplied.Spec)

		assert.Equal(t, expected, actual)
	})

	t.Run("removed one user and changed secret of the other", func(t *testing.T) {
		lastApplied = newScramReplicaSet(
			mdbv1.MongoDBUser{
				Name: "testUser",
				PasswordSecretRef: v1.SecretKeyReference{
					Name: "password-secret-name",
				},
				ScramCredentialsSecretName: "scram-credentials",
			},
			mdbv1.MongoDBUser{
				Name: "anotherUser",
				PasswordSecretRef: v1.SecretKeyReference{
					Name: "password-secret-name",
				},
				ScramCredentialsSecretName: "another-scram-credentials",
			},
		)

		current := newScramReplicaSet(mdbv1.MongoDBUser{
			Name: "testUser",
			PasswordSecretRef: v1.SecretKeyReference{
				Name: "password-secret-name",
			},
			ScramCredentialsSecretName: "scram-credentials-2",
		})

		expected := []string{"scram-credentials-scram-credentials", "another-scram-credentials-scram-credentials"}
		actual := getScramSecretsToDelete(current.Spec, lastApplied.Spec)

		assert.Equal(t, expected, actual)
	})
}

func TestReplicaSetReconcilerCleanupPemSecret(t *testing.T) {
	ctx := context.Background()
	lastAppliedSpec := mdbv1.MongoDBCommunitySpec{
		Security: mdbv1.Security{
			Authentication: mdbv1.Authentication{
				Modes: []mdbv1.AuthMode{"X509"},
			},
		},
	}
	mdb := mdbv1.MongoDBCommunity{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "my-rs",
			Namespace:   "my-ns",
			Annotations: map[string]string{},
		},
		Spec: mdbv1.MongoDBCommunitySpec{
			Members: 3,
			Version: "4.2.2",
			Security: mdbv1.Security{
				Authentication: mdbv1.Authentication{
					Modes: []mdbv1.AuthMode{"SCRAM"},
				},
				TLS: mdbv1.TLS{
					Enabled: true,
					CaConfigMap: &corev1.LocalObjectReference{
						Name: "caConfigMap",
					},
					CaCertificateSecret: &corev1.LocalObjectReference{
						Name: "certificateKeySecret",
					},
					CertificateKeySecret: corev1.LocalObjectReference{
						Name: "certificateKeySecret",
					},
				},
			},
		},
	}

	mgr := kubeClient.NewManager(ctx, &mdb)

	client := kubeClient.NewClient(mgr.GetClient())
	err := createAgentCertPemSecret(ctx, client, mdb, "CERT", "KEY", "")
	assert.NoError(t, err)

	r := NewReconciler(mgr, "fake-mongodbRepoUrl", "fake-mongodbImage", "ubi8", "fake-agentImage", "fake-versionUpgradeHookImage", "fake-readinessProbeImage")

	secret, err := r.client.GetSecret(ctx, mdb.AgentCertificatePemSecretNamespacedName())
	assert.NoError(t, err)
	assert.Equal(t, "CERT", string(secret.Data["tls.crt"]))
	assert.Equal(t, "KEY", string(secret.Data["tls.key"]))

	r.cleanupPemSecret(ctx, mdb.Spec, lastAppliedSpec, "my-ns")

	_, err = r.client.GetSecret(ctx, mdb.AgentCertificatePemSecretNamespacedName())
	assert.Error(t, err)
}

func TestReplicaSetReconcilerCleanupConnectionStringSecrets(t *testing.T) {
	user := func(name, secret, namespace string) mdbv1.MongoDBUser {
		return mdbv1.MongoDBUser{
			Name:                            name,
			ConnectionStringSecretName:      secret,
			ConnectionStringSecretNamespace: namespace,
		}
	}
	oldTarget := types.NamespacedName{Name: "connection-string", Namespace: "old-ns"}
	newTarget := types.NamespacedName{Name: "connection-string", Namespace: "new-ns"}
	secondTarget := types.NamespacedName{Name: "second-string", Namespace: "old-ns"}
	localTarget := types.NamespacedName{Name: "connection-string", Namespace: "my-ns"}

	for _, tc := range []struct {
		name     string
		previous []mdbv1.MongoDBUser
		current  []mdbv1.MongoDBUser
		keep     []types.NamespacedName
		remove   []types.NamespacedName
	}{
		{
			name:     "unchanged target",
			previous: []mdbv1.MongoDBUser{user("first", oldTarget.Name, oldTarget.Namespace)},
			current:  []mdbv1.MongoDBUser{user("first", oldTarget.Name, oldTarget.Namespace)},
			keep:     []types.NamespacedName{oldTarget},
		},
		{
			name:     "new user preserves existing target",
			previous: []mdbv1.MongoDBUser{user("first", oldTarget.Name, oldTarget.Namespace)},
			current:  []mdbv1.MongoDBUser{user("first", oldTarget.Name, oldTarget.Namespace), user("second", secondTarget.Name, secondTarget.Namespace)},
			keep:     []types.NamespacedName{oldTarget, secondTarget},
		},
		{
			name:     "removed user",
			previous: []mdbv1.MongoDBUser{user("first", oldTarget.Name, oldTarget.Namespace)},
			remove:   []types.NamespacedName{oldTarget},
		},
		{
			name:     "renamed secret",
			previous: []mdbv1.MongoDBUser{user("first", oldTarget.Name, oldTarget.Namespace)},
			current:  []mdbv1.MongoDBUser{user("first", secondTarget.Name, secondTarget.Namespace)},
			keep:     []types.NamespacedName{secondTarget},
			remove:   []types.NamespacedName{oldTarget},
		},
		{
			name:     "moved namespace",
			previous: []mdbv1.MongoDBUser{user("first", oldTarget.Name, oldTarget.Namespace)},
			current:  []mdbv1.MongoDBUser{user("first", newTarget.Name, newTarget.Namespace)},
			keep:     []types.NamespacedName{newTarget},
			remove:   []types.NamespacedName{oldTarget},
		},
		{
			name:     "removed namespace falls back to resource namespace",
			previous: []mdbv1.MongoDBUser{user("first", oldTarget.Name, oldTarget.Namespace)},
			current:  []mdbv1.MongoDBUser{user("first", localTarget.Name, "")},
			keep:     []types.NamespacedName{localTarget},
			remove:   []types.NamespacedName{oldTarget},
		},
		{
			name:     "explicit resource namespace is the same target",
			previous: []mdbv1.MongoDBUser{user("first", localTarget.Name, "")},
			current:  []mdbv1.MongoDBUser{user("first", localTarget.Name, localTarget.Namespace)},
			keep:     []types.NamespacedName{localTarget},
		},
		{
			name:     "renamed user reuses target",
			previous: []mdbv1.MongoDBUser{user("first", oldTarget.Name, oldTarget.Namespace)},
			current:  []mdbv1.MongoDBUser{user("renamed", oldTarget.Name, oldTarget.Namespace)},
			keep:     []types.NamespacedName{oldTarget},
		},
		{
			name:     "users swap targets",
			previous: []mdbv1.MongoDBUser{user("first", oldTarget.Name, oldTarget.Namespace), user("second", secondTarget.Name, secondTarget.Namespace)},
			current:  []mdbv1.MongoDBUser{user("first", secondTarget.Name, secondTarget.Namespace), user("second", oldTarget.Name, oldTarget.Namespace)},
			keep:     []types.NamespacedName{oldTarget, secondTarget},
		},
		{
			name:     "remaining user still uses shared target",
			previous: []mdbv1.MongoDBUser{user("first", oldTarget.Name, oldTarget.Namespace), user("second", oldTarget.Name, oldTarget.Namespace)},
			current:  []mdbv1.MongoDBUser{user("second", oldTarget.Name, oldTarget.Namespace)},
			keep:     []types.NamespacedName{oldTarget},
		},
		{
			name:     "external authentication user is cleaned up",
			previous: []mdbv1.MongoDBUser{{Name: "external-user", DB: "$external", ConnectionStringSecretName: oldTarget.Name, ConnectionStringSecretNamespace: oldTarget.Namespace}},
			remove:   []types.NamespacedName{oldTarget},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			mdb := newScramReplicaSet(tc.current...)
			mdb.UID = "mongodb-owner"
			mgr := kubeClient.NewManager(ctx, &mdb)
			r := NewReconciler(mgr, "fake-mongodbRepoUrl", "fake-mongodbImage", "ubi8", "fake-agentImage", "fake-versionUpgradeHookImage", "fake-readinessProbeImage")

			for _, target := range append(append([]types.NamespacedName{}, tc.keep...), tc.remove...) {
				secret := corev1.Secret{ObjectMeta: metav1.ObjectMeta{
					Name: target.Name, Namespace: target.Namespace,
					Annotations: map[string]string{
						connectionStringSecretOwnerNamespaceAnnotation: mdb.Namespace,
						connectionStringSecretOwnerNameAnnotation:      mdb.Name,
						connectionStringSecretOwnerUIDAnnotation:       string(mdb.UID),
					},
				}}
				require.NoError(t, mgr.Client.Create(ctx, &secret))
			}

			lastApplied := newScramReplicaSet(tc.previous...)
			require.NoError(t, r.cleanupConnectionStringSecrets(ctx, mdb.Spec, lastApplied.Spec, mdb))

			for _, target := range tc.keep {
				_, err := r.client.GetSecret(ctx, target)
				assert.NoError(t, err, "retained target %s must survive", target)
			}
			for _, target := range tc.remove {
				_, err := r.client.GetSecret(ctx, target)
				assert.True(t, apiErrors.IsNotFound(err), "obsolete target %s must be deleted, got %v", target, err)
			}
		})
	}
}

func TestReplicaSetReconcilerCleanupPreservesForeignSecrets(t *testing.T) {
	for _, tc := range []struct {
		name        string
		annotations map[string]string
	}{
		{name: "unmanaged secret"},
		{
			name: "recreated resource has a different UID",
			annotations: map[string]string{
				connectionStringSecretOwnerNamespaceAnnotation: "my-ns",
				connectionStringSecretOwnerNameAnnotation:      "my-rs",
				connectionStringSecretOwnerUIDAnnotation:       "previous-resource",
			},
		},
		{
			name: "another resource has the same name in another namespace",
			annotations: map[string]string{
				connectionStringSecretOwnerNamespaceAnnotation: "another-ns",
				connectionStringSecretOwnerNameAnnotation:      "my-rs",
				connectionStringSecretOwnerUIDAnnotation:       "mongodb-owner",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			mdb := newScramReplicaSet(mdbv1.MongoDBUser{
				Name: "test-user", ConnectionStringSecretName: "connection-string", ConnectionStringSecretNamespace: "other-ns",
			})
			mdb.UID = "mongodb-owner"
			mgr := kubeClient.NewManager(ctx, &mdb)
			r := NewReconciler(mgr, "fake-mongodbRepoUrl", "fake-mongodbImage", "ubi8", "fake-agentImage", "fake-versionUpgradeHookImage", "fake-readinessProbeImage")
			secret := corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "connection-string", Namespace: "other-ns", Annotations: tc.annotations},
				Data:       map[string][]byte{"preserve": []byte("original-value")},
			}
			require.NoError(t, mgr.Client.Create(ctx, &secret))

			require.NoError(t, r.cleanupConnectionStringSecrets(ctx, mdbv1.MongoDBCommunitySpec{}, mdb.Spec, mdb))

			actual, err := r.client.GetSecret(ctx, types.NamespacedName{Name: secret.Name, Namespace: secret.Namespace})
			require.NoError(t, err)
			assert.Equal(t, secret, actual)
		})
	}
}

func TestReplicaSetReconcilerDeleteCleansUpCrossNamespaceConnectionStringSecrets(t *testing.T) {
	for _, tc := range []struct {
		name              string
		lastSnapshot      bool
		oldNamespace      string
		malformedSnapshot bool
		externalUser      bool
		missingSecret     bool
		failOperation     string
	}{
		{name: "completed reconciliation", lastSnapshot: true},
		{name: "partial first reconciliation has no snapshot"},
		{name: "current and previously applied namespaces", lastSnapshot: true, oldNamespace: "old-ns"},
		{name: "external user on partial first reconciliation", externalUser: true},
		{name: "missing secret is already cleaned up", lastSnapshot: true, missingSecret: true},
		{name: "get failure keeps finalizer for retry", lastSnapshot: true, failOperation: "get"},
		{name: "delete failure keeps finalizer for retry", lastSnapshot: true, failOperation: "delete"},
		{name: "malformed snapshot keeps finalizer", malformedSnapshot: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			mdb := newScramReplicaSet(mdbv1.MongoDBUser{
				Name: "test-user", ConnectionStringSecretName: "connection-string", ConnectionStringSecretNamespace: "other-ns",
			})
			mdb.UID = "mongodb-owner"
			mdb.Kind = "MongoDBCommunity"
			mdb.APIVersion = mdbv1.GroupVersion.String()
			mdb.Finalizers = []string{mongoDBCommunityFinalizer}
			now := metav1.NewTime(time.Now())
			mdb.DeletionTimestamp = &now
			if tc.externalUser {
				mdb.Spec.Users[0].DB = "$external"
			}
			if tc.lastSnapshot {
				previous := mdb.Spec.DeepCopy()
				if tc.oldNamespace != "" {
					previous.Users[0].ConnectionStringSecretNamespace = tc.oldNamespace
				}
				encoded, err := json.Marshal(previous)
				require.NoError(t, err)
				mdb.Annotations[lastSuccessfulConfiguration] = string(encoded)
			}
			if tc.malformedSnapshot {
				mdb.Annotations[lastSuccessfulConfiguration] = "{broken"
			}

			scheme := runtime.NewScheme()
			require.NoError(t, corev1.AddToScheme(scheme))
			require.NoError(t, mdbv1.AddToScheme(scheme))
			objects := []client.Object{&mdb}
			var targets []types.NamespacedName
			if !tc.missingSecret {
				targets = append(targets, types.NamespacedName{Name: "connection-string", Namespace: "other-ns"})
			}
			if tc.oldNamespace != "" {
				targets = append(targets, types.NamespacedName{Name: "connection-string", Namespace: tc.oldNamespace})
			}
			for _, target := range targets {
				objects = append(objects, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
					Name: target.Name, Namespace: target.Namespace, UID: types.UID("secret-" + target.Namespace),
					Annotations: map[string]string{
						connectionStringSecretOwnerNamespaceAnnotation: mdb.Namespace,
						connectionStringSecretOwnerNameAnnotation:      mdb.Name,
						connectionStringSecretOwnerUIDAnnotation:       string(mdb.UID),
					},
				}})
			}

			fail := tc.failOperation != ""
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).WithInterceptorFuncs(interceptor.Funcs{
				Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*corev1.Secret); ok && fail && tc.failOperation == "get" {
						return fmt.Errorf("injected secret get failure")
					}
					return cl.Get(ctx, key, obj, opts...)
				},
				Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					if secret, ok := obj.(*corev1.Secret); ok {
						options := &client.DeleteOptions{}
						for _, option := range opts {
							option.ApplyToDelete(options)
						}
						require.NotNil(t, options.Preconditions, "cleanup must not delete a replacement Secret")
						require.NotNil(t, options.Preconditions.UID)
						assert.Equal(t, secret.UID, *options.Preconditions.UID)
						if fail && tc.failOperation == "delete" {
							return fmt.Errorf("injected secret delete failure")
						}
					}
					return cl.Delete(ctx, obj, opts...)
				},
			}).Build()
			mgr := kubeClient.NewManagerWithClient(cl)
			r := NewReconciler(mgr, "fake-mongodbRepoUrl", "fake-mongodbImage", "ubi8", "fake-agentImage", "fake-versionUpgradeHookImage", "fake-readinessProbeImage")
			request := reconcile.Request{NamespacedName: types.NamespacedName{Name: mdb.Name, Namespace: mdb.Namespace}}

			result, err := r.Reconcile(ctx, request)
			if fail || tc.malformedSnapshot {
				assert.True(t, err != nil || result.RequeueAfter > 0, "failed cleanup must be retried")
				persisted := &mdbv1.MongoDBCommunity{}
				require.NoError(t, cl.Get(ctx, request.NamespacedName, persisted))
				assert.Contains(t, persisted.Finalizers, mongoDBCommunityFinalizer)
				fail = false
				for _, target := range targets {
					require.NoError(t, cl.Get(ctx, target, &corev1.Secret{}))
				}
				if tc.malformedSnapshot {
					return
				}
				_, err = r.Reconcile(ctx, request)
			}
			require.NoError(t, err)
			for _, target := range targets {
				assert.True(t, apiErrors.IsNotFound(cl.Get(ctx, target, &corev1.Secret{})), "owned secret %s must be deleted", target)
			}
			assert.True(t, apiErrors.IsNotFound(cl.Get(ctx, request.NamespacedName, &mdbv1.MongoDBCommunity{})), "finalizer must be released after cleanup")
		})
	}
}
