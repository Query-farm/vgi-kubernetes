#!/usr/bin/env bash
# Run a command against a throwaway kind cluster seeded with test/fixture.
#
#   ci/kind-fixture.sh <command...>
#
# Creates the kind cluster `vgi-kubernetes-test` (its kubeconfig goes to a
# temporary file; ~/.kube/config is never touched), loads busybox, applies
# test/fixture/*.yaml (metrics-server included), waits for the fixture to
# settle, creates a token for the read-only ServiceAccount vgi-fixture/vgi-sql
# and exports:
#
#   KUBERNETES_FIXTURE_SERVER   API server URL
#   KUBERNETES_FIXTURE_TOKEN    bearer token of vgi-sql (get/list/watch only)
#   KUBERNETES_FIXTURE_CA       base64 CA certificate of the cluster
#   KUBERNETES_FIXTURE_KUBECONFIG  base64 kubeconfig (admin) of the cluster
#
# then runs the command and deletes the cluster. If KUBERNETES_FIXTURE_SERVER
# is already set, the command just runs. KIND_KEEP=1 keeps the cluster (and
# reuses an existing one). Needs docker, kind and kubectl on PATH (or $KIND).
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
if [ -n "${KUBERNETES_FIXTURE_SERVER:-}" ]; then exec "$@"; fi

KIND="${KIND:-$(command -v kind || echo "$HOME/go/bin/kind")}"
CLUSTER="${KIND_CLUSTER:-vgi-kubernetes-test}"
IMAGE="busybox:1.36"
WORK="$(mktemp -d -t vgi-kubernetes-kind.XXXXXX)"
export KUBECONFIG="$WORK/kubeconfig"
created=0
cleanup() {
  if [ "$created" = 1 ] && [ "${KIND_KEEP:-0}" != 1 ]; then
    "$KIND" delete cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG" >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

if "$KIND" get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  [ "${KIND_KEEP:-0}" = 1 ] || { echo "kind cluster $CLUSTER already exists; delete it or set KIND_KEEP=1 to reuse it" >&2; exit 1; }
  "$KIND" export kubeconfig --name "$CLUSTER" --kubeconfig "$KUBECONFIG" >/dev/null
else
  "$KIND" create cluster --name "$CLUSTER" --config "$REPO/test/fixture/kind.yaml" --kubeconfig "$KUBECONFIG" --wait 180s
  created=1
fi

docker image inspect "$IMAGE" >/dev/null 2>&1 || docker pull -q "$IMAGE" >/dev/null
"$KIND" load docker-image "$IMAGE" --name "$CLUSTER" >/dev/null

K="kubectl --kubeconfig $KUBECONFIG"
$K apply -f "$REPO/test/fixture/manifests.yaml" -f "$REPO/test/fixture/reader.yaml" -f "$REPO/test/fixture/metrics-server.yaml" >/dev/null
$K wait --for condition=established --timeout 60s crd/widgets.fixture.vgi.query.farm >/dev/null
$K apply -f "$REPO/test/fixture/widgets.yaml" >/dev/null
$K -n vgi-fixture rollout status deployment/web --timeout 180s >/dev/null
$K -n vgi-fixture rollout status statefulset/db --timeout 180s >/dev/null
$K -n vgi-fixture rollout status daemonset/agent --timeout 180s >/dev/null
$K -n vgi-fixture wait --for condition=complete --timeout 180s job/pi >/dev/null
$K -n vgi-fixture wait --for jsonpath='{.status.phase}'=Succeeded --timeout 180s pod/hello >/dev/null
# The crash-looping pod: wait until it has restarted at least once and is in back-off.
for _ in $(seq 1 90); do
  reason="$($K -n vgi-fixture get pod crashy -o jsonpath='{.status.containerStatuses[0].state.waiting.reason}' 2>/dev/null || true)"
  restarts="$($K -n vgi-fixture get pod crashy -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null || echo 0)"
  [ "$reason" = "CrashLoopBackOff" ] && [ "${restarts:-0}" -ge 1 ] && break
  sleep 2
done
[ "$reason" = "CrashLoopBackOff" ] || { echo "crashy never reached CrashLoopBackOff" >&2; $K -n vgi-fixture describe pod crashy >&2; exit 1; }

# metrics-server: wait until it reports usage for the web pods.
$K -n kube-system rollout status deployment/metrics-server --timeout 180s >/dev/null
for _ in $(seq 1 60); do
  [ "$($K top pods -n vgi-fixture -l app=web --no-headers 2>/dev/null | wc -l | tr -d ' ')" = 2 ] && break
  sleep 3
done

export KUBERNETES_FIXTURE_SERVER="$($K config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
export KUBERNETES_FIXTURE_CA="$($K config view --minify --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')"
export KUBERNETES_FIXTURE_TOKEN="$($K -n vgi-fixture create token vgi-sql --duration 2h)"
export KUBERNETES_FIXTURE_KUBECONFIG="$(base64 < "$KUBECONFIG" | tr -d '\n')"
echo "kind fixture $CLUSTER at $KUBERNETES_FIXTURE_SERVER" >&2
"$@"
