{{/* Common naming and label helpers. */}}

{{- define "il.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "il.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "il.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "il.labels" -}}
app.kubernetes.io/name: {{ include "il.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end -}}

{{/* Per-component selector labels. */}}
{{- define "il.selector" -}}
app.kubernetes.io/name: {{ include "il.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Resolved backing-service endpoints: explicit override wins, else the
     in-chart service DNS name. Keeping this in one place means a template can
     never disagree with itself about where Kafka is. */}}
{{- define "il.kafkaBootstrap" -}}
{{- if .Values.endpoints.kafkaBootstrap -}}{{ .Values.endpoints.kafkaBootstrap }}
{{- else -}}{{ .Release.Name }}-redpanda:9092{{- end -}}
{{- end -}}

{{- define "il.redisUrl" -}}
{{- if .Values.endpoints.redisUrl -}}{{ .Values.endpoints.redisUrl }}
{{- else -}}redis://{{ .Release.Name }}-redis:6379/0{{- end -}}
{{- end -}}

{{- define "il.postgresDsn" -}}
{{- if .Values.endpoints.postgresDsn -}}{{ .Values.endpoints.postgresDsn }}
{{- else -}}postgresql://{{ .Values.postgres.user }}:{{ .Values.postgres.password }}@{{ .Release.Name }}-postgres:5432/{{ .Values.postgres.database }}{{- end -}}
{{- end -}}

{{/* An initContainer that blocks until the enabled in-chart dependencies accept
     TCP, so the app containers start clean instead of crash-looping until the
     database is ready. Reuses the app image (already present in the cluster) so
     there is no extra pull. */}}
{{- define "il.waitInitContainer" -}}
- name: wait-deps
  image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
  imagePullPolicy: {{ .Values.image.pullPolicy }}
  command:
    - python
    - -c
    - |
      import socket, time
      targets = []
      {{- if .Values.postgres.enabled }}
      targets.append(("{{ .Release.Name }}-postgres", 5432))
      {{- end }}
      {{- if .Values.redpanda.enabled }}
      targets.append(("{{ .Release.Name }}-redpanda", 9092))
      {{- end }}
      {{- if .Values.redis.enabled }}
      targets.append(("{{ .Release.Name }}-redis", 6379))
      {{- end }}
      for host, port in targets:
          while True:
              try:
                  socket.create_connection((host, port), timeout=2).close()
                  print("ready", host, port, flush=True); break
              except OSError:
                  print("waiting", host, port, flush=True); time.sleep(2)
{{- end -}}

{{/* Effective provider base URL: the in-cluster mock when the dev provider is
     on, otherwise the configured real endpoint. */}}
{{- define "il.providerBaseUrl" -}}
{{- if .Values.chaosProvider.enabled -}}http://{{ .Release.Name }}-chaos-provider:9000
{{- else -}}{{ .Values.provider.baseUrl }}{{- end -}}
{{- end -}}

{{- define "il.providerSecretName" -}}
{{- if .Values.provider.existingSecret -}}{{ .Values.provider.existingSecret }}
{{- else -}}{{ .Release.Name }}-provider{{- end -}}
{{- end -}}
