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

{{- define "il.providerSecretName" -}}
{{- if .Values.provider.existingSecret -}}{{ .Values.provider.existingSecret }}
{{- else -}}{{ .Release.Name }}-provider{{- end -}}
{{- end -}}
