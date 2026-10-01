# Kimiafarma Kviklet application rollout

`kviklet-gzip.patch.yaml` records the immutable image and readiness gate for the
existing `7s-tools/kviklet` deployment in the Kimiafarma cluster. It is a
strategic merge patch, not a standalone Deployment manifest. Existing database
credentials, service routing and other container configuration remain owned by
the existing deployment configuration.

After verifying the exact Kimiafarma context and receiving rollout authority:

```sh
kubectl --context "$KIMIAFARMA_CONTEXT" -n 7s-tools patch deployment kviklet \
  --type strategic --patch-file deployments/kimiafarma/kviklet-gzip.patch.yaml \
  --dry-run=server
kubectl --context "$KIMIAFARMA_CONTEXT" -n 7s-tools patch deployment kviklet \
  --type strategic --patch-file deployments/kimiafarma/kviklet-gzip.patch.yaml
kubectl --context "$KIMIAFARMA_CONTEXT" -n 7s-tools rollout status deployment/kviklet
```

Read back the pod image ID, source revision and public `/api/health` response.
Refresh the browser so it loads the gzip-capable frontend, then verify that the
previously failing result completes with `compression=gzip` on its WebSocket
URL. Readiness alone does not prove a full query result is delivered.

The previous rollback image is
`docker.io/bluespy/kviklet-fix@sha256:3b93afd3c8c5cedb710b4dcf3673977444a1ab7828b6ee6723189e1469851332`,
revision `288bf71` / version `deploy-288bf71`. Restore that image and source
annotations if the candidate fails. Its `/api/health` endpoint supports the
readiness gate; the gate can remain in place during an image rollback.

The workload has no observed GitOps controller. Any future adoption into a
controller must preserve this image/protocol decision and move ownership of the
patch deliberately.
