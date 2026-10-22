# Downstream patches

This fork tracks `mongodb/mongodb-kubernetes` and maintains the following
MongoDB Community operator fixes.

## Connection-string Secrets in another namespace

Kubernetes does not allow a namespaced resource to own a Secret in another
namespace through an owner reference. Generated connection-string Secrets use
resource namespace, name and UID annotations instead when placed elsewhere.
Secrets in the resource's own namespace keep their normal owner reference.

The controller cleans up obsolete targets from the last successful
configuration. On resource deletion, a finalizer cleans up generated Secrets
referenced by either the current configuration or the last successful
configuration. Ownership checks protect unrelated Secrets, UID preconditions
protect replacement Secrets, and failures retain the finalizer for retry. The
update predicate permits the transition into deletion so cleanup can start.

Cleanup does not track every intermediate configuration. For example, suppose
the last successful configuration used target A. A later reconciliation creates
target B but fails before saving a successful configuration. If the user changes
the target to C before reconciliation succeeds, deletion checks C and A;
target B can remain orphaned. The same limitation applies before the first
successful reconciliation. This patch does not discover all generated Secrets
by their ownership annotations or keep a separate history of pending targets.

The original fixes are preserved on the
[`fix-connection-string-secret-namespace` branch](https://github.com/zootest-dev/mongodb-kubernetes/tree/fix-connection-string-secret-namespace).

After updating from upstream, run the controller and predicate unit tests:

```sh
go test ./mongodb-community-operator/controllers ./mongodb-community-operator/controllers/predicates -count=1 -timeout=3m
```
