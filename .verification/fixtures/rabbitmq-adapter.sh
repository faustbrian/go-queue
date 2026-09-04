#!/usr/bin/env bash

set -euo pipefail

operation="${1:-}"
if [[ "${operation}" != start && "${operation}" != stop ]]; then
    printf '%s\n' 'usage: ci-rabbitmq-adapter.sh <start|stop>' >&2
    exit 2
fi

runtime_temp="${RUNNER_TEMP:-${TMPDIR:-/tmp}}"
if [[ ! -d "${runtime_temp}" ]]; then
    printf '%s\n' 'RabbitMQ task root does not exist' >&2
    exit 1
fi

cleanup_task_root() {
    local task_root="$1"
    case "${task_root}" in
        "${runtime_temp}"/go-queue-rabbitmq-adapter.*) ;;
        *)
            printf '%s\n' 'refusing to clean an unexpected RabbitMQ task root' >&2
            return 1
            ;;
    esac
    if [[ -d "${task_root}" ]]; then
        chmod -R u+w "${task_root}"
        find "${task_root}" -depth -delete
    fi
}

if [[ "${operation}" == stop ]]; then
    task_root="${RABBITMQ_ADAPTER_TASK_ROOT:-}"
    [[ -n "${task_root}" && -f "${task_root}/container-name" && -f "${task_root}/owner-token" ]] || exit 0
    container_name="$(<"${task_root}/container-name")"
    owner_token="$(<"${task_root}/owner-token")"
    if [[ ! "${owner_token}" =~ ^[0-9a-f]{24}$ || "${container_name}" != "go-queue-rabbitmq-adapter-${owner_token}" ]]; then
        printf '%s\n' 'refusing to remove an unexpected RabbitMQ container' >&2
        exit 1
    fi
    if docker inspect "${container_name}" >/dev/null 2>&1; then
        container_owner="$(docker inspect --format '{{ index .Config.Labels "com.faustbrian.owner" }}' "${container_name}")"
        if [[ "${container_owner}" != "${owner_token}" ]]; then
            printf '%s\n' 'refusing to remove an unowned RabbitMQ container' >&2
            exit 1
        fi
        docker rm --force "${container_name}" >/dev/null
    fi
    cleanup_task_root "${task_root}"
    exit 0
fi

state_file="${RABBITMQ_ADAPTER_STATE_FILE:-}"
if [[ -z "${state_file}" ]]; then
    printf '%s\n' 'RABBITMQ_ADAPTER_STATE_FILE is required' >&2
    exit 1
fi

owner_token="$(openssl rand -hex 12)"
if [[ ! "${owner_token}" =~ ^[0-9a-f]{24}$ ]]; then
    printf '%s\n' 'RabbitMQ owner token must be hexadecimal' >&2
    exit 1
fi
container_name="go-queue-rabbitmq-adapter-${owner_token}"
task_root="$(mktemp -d "${runtime_temp}/go-queue-rabbitmq-adapter.XXXXXX")"
cleanup_and_exit() {
    local result="$1"
    trap - EXIT HUP INT TERM
    if docker inspect "${container_name}" >/dev/null 2>&1; then
        container_owner="$(docker inspect --format '{{ index .Config.Labels "com.faustbrian.owner" }}' "${container_name}")"
        if [[ "${container_owner}" == "${owner_token}" ]]; then
            if (( result != 0 )); then
                docker logs "${container_name}" 2>&1 |
                    sed \
                        -e "s/${bootstrap_password:-}/[REDACTED]/g" \
                        -e "s/${client_password:-}/[REDACTED]/g" \
                        -e "s/${erlang_cookie:-}/[REDACTED]/g" >&2 || true
            fi
            docker rm --force "${container_name}" >/dev/null 2>&1 || true
        else
            printf '%s\n' 'refusing to remove an unowned RabbitMQ container during cleanup' >&2
        fi
    fi
    cleanup_task_root "${task_root}"
    exit "${result}"
}
handle_exit() {
    cleanup_and_exit "$?"
}
trap handle_exit EXIT
trap 'cleanup_and_exit 129' HUP
trap 'cleanup_and_exit 130' INT
trap 'cleanup_and_exit 143' TERM

bootstrap_password="$(openssl rand -hex 24)"
client_password="$(openssl rand -hex 24)"
erlang_cookie="$(openssl rand -hex 24)"

mkdir -p "${task_root}/tls" "${task_root}/rabbitmq-data"
chmod 0700 "${task_root}/rabbitmq-data"
printf '%s\n' "${container_name}" >"${task_root}/container-name"
printf '%s\n' "${owner_token}" >"${task_root}/owner-token"

openssl req -x509 -newkey rsa:2048 -sha256 -days 1 -nodes \
    -subj '/CN=go-queue-rabbitmq-adapter-ci-ca' \
    -keyout "${task_root}/tls/ca-key.pem" \
    -out "${task_root}/tls/ca.pem" >/dev/null 2>&1
openssl req -newkey rsa:2048 -sha256 -nodes \
    -subj '/CN=localhost' \
    -keyout "${task_root}/tls/server-key.pem" \
    -out "${task_root}/tls/server.csr" >/dev/null 2>&1
printf '%s\n' \
    'subjectAltName=DNS:localhost,IP:127.0.0.1' \
    'extendedKeyUsage=serverAuth' \
    >"${task_root}/tls/server.ext"
openssl x509 -req -sha256 -days 1 \
    -in "${task_root}/tls/server.csr" \
    -CA "${task_root}/tls/ca.pem" \
    -CAkey "${task_root}/tls/ca-key.pem" \
    -CAcreateserial \
    -extfile "${task_root}/tls/server.ext" \
    -out "${task_root}/tls/server.pem" >/dev/null 2>&1
chmod 0644 "${task_root}/tls/"*.pem

cat >"${task_root}/rabbitmq.conf" <<'EOF'
listeners.tcp = none
listeners.ssl.default = 5671
management.tcp.ip = 0.0.0.0
management.tcp.port = 15672
loopback_users.guest = false
ssl_options.cacertfile = /etc/rabbitmq/tls/ca.pem
ssl_options.certfile = /etc/rabbitmq/tls/server.pem
ssl_options.keyfile = /etc/rabbitmq/tls/server-key.pem
ssl_options.verify = verify_none
ssl_options.fail_if_no_peer_cert = false
ssl_options.versions.1 = tlsv1.3
ssl_options.versions.2 = tlsv1.2
EOF

bootstrap_user='ci-bootstrap'
client_user='ci-adapter'
vhost='/go-queue-adapter-ci'
encoded_vhost='%2Fgo-queue-adapter-ci'
image='rabbitmq:4.3.5-management-alpine@sha256:7224161872a48060e980a611f4778ad18168f00cfa974cab30604dbd855511dc'

docker run --detach \
    --name "${container_name}" \
    --user "$(id -u):$(id -g)" \
    --label 'com.faustbrian.task=go-queue-rabbitmq-adapter-ci' \
    --label "com.faustbrian.owner=${owner_token}" \
    --env "RABBITMQ_DEFAULT_USER=${bootstrap_user}" \
    --env "RABBITMQ_DEFAULT_PASS=${bootstrap_password}" \
    --env "RABBITMQ_DEFAULT_VHOST=${vhost}" \
    --env "RABBITMQ_ERLANG_COOKIE=${erlang_cookie}" \
    --mount "type=bind,source=${task_root}/rabbitmq.conf,target=/etc/rabbitmq/rabbitmq.conf,readonly" \
    --mount "type=bind,source=${task_root}/tls,target=/etc/rabbitmq/tls,readonly" \
    --mount "type=bind,source=${task_root}/rabbitmq-data,target=/var/lib/rabbitmq" \
    --publish 127.0.0.1::5671 \
    --publish 127.0.0.1::15672 \
    "${image}" >/dev/null

for _ in $(seq 1 120); do
    if docker exec "${container_name}" rabbitmq-diagnostics -q ping >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
docker exec "${container_name}" rabbitmq-diagnostics -q ping >/dev/null
test "$(docker exec "${container_name}" rabbitmqctl version)" = '4.3.5'
amqp_port="$(docker port "${container_name}" 5671/tcp | awk -F: 'NR == 1 { print $NF }')"
management_port="$(docker port "${container_name}" 15672/tcp | awk -F: 'NR == 1 { print $NF }')"
[[ "${amqp_port}" =~ ^[0-9]+$ ]]
[[ "${management_port}" =~ ^[0-9]+$ ]]

management_url="http://127.0.0.1:${management_port}/api"
for _ in $(seq 1 60); do
    if curl --fail --silent --show-error \
        --user "${bootstrap_user}:${bootstrap_password}" \
        "${management_url}/overview" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
curl --fail --silent --show-error \
    --user "${bootstrap_user}:${bootstrap_password}" \
    "${management_url}/overview" >/dev/null

put_json() {
    local endpoint="$1"
    local body="$2"
    curl --fail --silent --show-error \
        --user "${bootstrap_user}:${bootstrap_password}" \
        --header 'content-type: application/json' \
        --request PUT \
        --data "${body}" \
        "${management_url}/${endpoint}" >/dev/null
}

post_json() {
    local endpoint="$1"
    local body="$2"
    curl --fail --silent --show-error \
        --user "${bootstrap_user}:${bootstrap_password}" \
        --header 'content-type: application/json' \
        --request POST \
        --data "${body}" \
        "${management_url}/${endpoint}" >/dev/null
}

put_json "users/${client_user}" \
    "$(jq -cn --arg password "${client_password}" '{password: $password, tags: ""}')"
put_json "permissions/${encoded_vhost}/${client_user}" \
    '{"configure":"^$","write":"^go-queue-adapter\\.(events|dead)$","read":"^go-queue-adapter\\.(jobs|retry|dead)$"}'
put_json "exchanges/${encoded_vhost}/go-queue-adapter.events" \
    '{"type":"direct","auto_delete":false,"durable":true,"internal":false,"arguments":{}}'
put_json "exchanges/${encoded_vhost}/go-queue-adapter.dead" \
    '{"type":"direct","auto_delete":false,"durable":true,"internal":false,"arguments":{}}'
put_json "queues/${encoded_vhost}/go-queue-adapter.jobs" \
    '{"auto_delete":false,"durable":true,"arguments":{"x-queue-type":"classic"}}'
put_json "queues/${encoded_vhost}/go-queue-adapter.retry" \
    '{"auto_delete":false,"durable":true,"arguments":{"x-queue-type":"quorum"}}'
put_json "queues/${encoded_vhost}/go-queue-adapter.dead" \
    '{"auto_delete":false,"durable":true,"arguments":{"x-queue-type":"quorum"}}'
post_json "bindings/${encoded_vhost}/e/go-queue-adapter.events/q/go-queue-adapter.jobs" \
    '{"routing_key":"jobs","arguments":{}}'
post_json "bindings/${encoded_vhost}/e/go-queue-adapter.events/q/go-queue-adapter.retry" \
    '{"routing_key":"retry","arguments":{}}'
post_json "bindings/${encoded_vhost}/e/go-queue-adapter.dead/q/go-queue-adapter.dead" \
    '{"routing_key":"dead","arguments":{}}'

docker exec -i "${container_name}" openssl s_client \
    -connect '127.0.0.1:5671' \
    -servername localhost \
    -CAfile /etc/rabbitmq/tls/ca.pem \
    -tls1_2 </dev/null >/dev/null 2>&1
docker exec -i "${container_name}" openssl s_client \
    -connect '127.0.0.1:5671' \
    -servername localhost \
    -CAfile /etc/rabbitmq/tls/ca.pem \
    -tls1_3 </dev/null >/dev/null 2>&1

jq -n \
    --argjson port "${amqp_port}" \
    --arg vhost "${vhost}" \
    --arg username "${client_user}" \
    --arg password "${client_password}" \
    --arg root_ca_file "${task_root}/tls/ca.pem" \
    '{
        endpoints: [{host: "127.0.0.1", port: $port}],
        virtual_host: $vhost,
        username: $username,
        password: $password,
        tls: {server_name: "localhost", root_ca_file: $root_ca_file},
        exchange: "go-queue-adapter.events",
        jobs: {name: "go-queue-adapter.jobs", routing_key: "jobs", queue_type: "classic"},
        retry: {name: "go-queue-adapter.retry", routing_key: "retry", queue_type: "quorum"},
        dead_letter: {exchange: "go-queue-adapter.dead", queue_name: "go-queue-adapter.dead", routing_key: "dead"},
        unroutable_routing_key: "intentionally-unbound",
        missing_queue: "go-queue-adapter.missing"
    }' >"${task_root}/live-broker.json"
chmod 0600 "${task_root}/live-broker.json"

jq -n \
    --arg task_root "${task_root}" \
    --arg live_config "${task_root}/live-broker.json" \
    '{task_root: $task_root, live_config: $live_config}' >"${state_file}"
trap - EXIT HUP INT TERM
