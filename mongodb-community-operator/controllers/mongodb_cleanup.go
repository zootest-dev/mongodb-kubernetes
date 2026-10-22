package controllers

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/types"
	k8sClient "sigs.k8s.io/controller-runtime/pkg/client"

	apiErrors "k8s.io/apimachinery/pkg/api/errors"

	mdbv1 "github.com/mongodb/mongodb-kubernetes/mongodb-community-operator/api/v1"
	"github.com/mongodb/mongodb-kubernetes/pkg/util/constants"
)

// cleanupPemSecret cleans up the old pem secret generated for the agent certificate.
func (r *ReplicaSetReconciler) cleanupPemSecret(ctx context.Context, currentMDBSpec mdbv1.MongoDBCommunitySpec, lastAppliedMDBSpec mdbv1.MongoDBCommunitySpec, namespace string) {
	if currentMDBSpec.GetAgentAuthMode() == lastAppliedMDBSpec.GetAgentAuthMode() {
		return
	}

	if !currentMDBSpec.IsAgentX509() && lastAppliedMDBSpec.IsAgentX509() {
		agentCertSecret := lastAppliedMDBSpec.GetAgentCertificateRef()
		if err := r.client.DeleteSecret(ctx, types.NamespacedName{
			Namespace: namespace,
			Name:      agentCertSecret + "-pem",
		}); err != nil {
			if apiErrors.IsNotFound(err) {
				r.log.Debugf("Agent pem file secret %s-pem was already deleted", agentCertSecret)
			} else {
				r.log.Warnf("Could not cleanup old agent pem file %s-pem: %s", agentCertSecret, err)
			}
		}
	}
}

// cleanupScramSecrets cleans up old scram secrets based on the last successful applied mongodb spec.
func (r *ReplicaSetReconciler) cleanupScramSecrets(ctx context.Context, currentMDBSpec mdbv1.MongoDBCommunitySpec, lastAppliedMDBSpec mdbv1.MongoDBCommunitySpec, namespace string) {
	secretsToDelete := getScramSecretsToDelete(currentMDBSpec, lastAppliedMDBSpec)

	for _, s := range secretsToDelete {
		if err := r.client.DeleteSecret(ctx, types.NamespacedName{
			Name:      s,
			Namespace: namespace,
		}); err != nil {
			r.log.Warnf("Could not cleanup old secret %s: %s", s, err)
		} else {
			r.log.Debugf("Sucessfully cleaned up secret: %s", s)
		}
	}
}

// cleanupConnectionStringSecrets cleans up old connection string secrets based on the last successful applied MongoDB spec.
func (r *ReplicaSetReconciler) cleanupConnectionStringSecrets(ctx context.Context, currentMDBSpec mdbv1.MongoDBCommunitySpec, lastAppliedMDBSpec mdbv1.MongoDBCommunitySpec, mdb mdbv1.MongoDBCommunity) error {
	secretsToDelete := getConnectionStringSecretsToDelete(currentMDBSpec, lastAppliedMDBSpec, mdb.Namespace, mdb.Name)

	for _, s := range secretsToDelete {
		existingSecret, err := r.client.GetSecret(ctx, s)
		if apiErrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("get connection string secret %s/%s for cleanup: %w", s.Namespace, s.Name, err)
		}

		if !isManagedConnectionStringSecret(existingSecret, mdb) {
			r.log.Debugf("Leaving secret %s/%s because it is not owned by this MongoDBCommunity", s.Namespace, s.Name)
			continue
		}

		// Do not delete a replacement Secret if it changed after the ownership check.
		if err := r.client.Delete(ctx, &existingSecret, k8sClient.Preconditions{UID: &existingSecret.UID}); err != nil && !apiErrors.IsNotFound(err) {
			return fmt.Errorf("delete connection string secret %s/%s: %w", s.Namespace, s.Name, err)
		}
	}
	return nil
}

func getScramSecretsToDelete(currentMDBSpec mdbv1.MongoDBCommunitySpec, lastAppliedMDBSpec mdbv1.MongoDBCommunitySpec) []string {
	type user struct {
		db   string
		name string
	}
	m := map[user]string{}
	var secretsToDelete []string

	for _, mongoDBUser := range currentMDBSpec.Users {
		if mongoDBUser.DB == constants.ExternalDB {
			continue
		}
		m[user{db: mongoDBUser.DB, name: mongoDBUser.Name}] = mongoDBUser.GetScramCredentialsSecretName()
	}

	for _, mongoDBUser := range lastAppliedMDBSpec.Users {
		if mongoDBUser.DB == constants.ExternalDB {
			continue
		}
		currentScramSecretName, ok := m[user{db: mongoDBUser.DB, name: mongoDBUser.Name}]
		if !ok { // not used anymore
			secretsToDelete = append(secretsToDelete, mongoDBUser.GetScramCredentialsSecretName())
		} else if currentScramSecretName != mongoDBUser.GetScramCredentialsSecretName() { // have changed
			secretsToDelete = append(secretsToDelete, mongoDBUser.GetScramCredentialsSecretName())
		}
	}
	return secretsToDelete
}

func getConnectionStringSecretsToDelete(currentMDBSpec mdbv1.MongoDBCommunitySpec, lastAppliedMDBSpec mdbv1.MongoDBCommunitySpec, resourceNamespace string, resourceName string) []types.NamespacedName {
	currentSecrets := map[types.NamespacedName]struct{}{}
	scheduledSecrets := map[types.NamespacedName]struct{}{}
	var secretsToDelete []types.NamespacedName

	for _, mongoDBUser := range currentMDBSpec.Users {
		currentSecrets[types.NamespacedName{
			Name:      mongoDBUser.GetConnectionStringSecretName(resourceName),
			Namespace: mongoDBUser.GetConnectionStringSecretNamespace(resourceNamespace),
		}] = struct{}{}
	}

	for _, mongoDBUser := range lastAppliedMDBSpec.Users {
		lastAppliedSecret := types.NamespacedName{
			Name:      mongoDBUser.GetConnectionStringSecretName(resourceName),
			Namespace: mongoDBUser.GetConnectionStringSecretNamespace(resourceNamespace),
		}

		// A target can still be needed by another user after a rename or reassignment.
		if _, retained := currentSecrets[lastAppliedSecret]; retained {
			continue
		}
		if _, scheduled := scheduledSecrets[lastAppliedSecret]; scheduled {
			continue
		}
		secretsToDelete = append(secretsToDelete, lastAppliedSecret)
		scheduledSecrets[lastAppliedSecret] = struct{}{}
	}
	return secretsToDelete
}
