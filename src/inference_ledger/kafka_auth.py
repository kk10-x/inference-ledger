"""Kafka client configuration, including AWS MSK IAM authentication.

MSK Serverless supports *only* IAM auth — there is no SASL password to put in a
Secret. Clients authenticate with a short-lived signed token derived from the
caller's AWS identity, which on EKS comes from the pod's IRSA role. That is
strictly better than a static credential (nothing long-lived to leak) but it
means the client config differs from a plaintext broker, so it lives here once
rather than being duplicated across the producer and the consumer.

Local development against Redpanda needs none of this: leave ``sasl_iam``
disabled and the config stays PLAINTEXT.
"""

from __future__ import annotations

import logging

log = logging.getLogger("kafka_auth")


def _oauth_token_provider(region: str):
    """Return a confluent-kafka ``oauth_cb`` that mints MSK IAM tokens.

    Imported lazily so that neither local development nor CI needs the AWS
    signer library installed — it is only required when actually talking to MSK.
    """
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    def oauth_cb(_config: str) -> tuple[str, float]:
        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
        # confluent-kafka wants absolute expiry in seconds; the signer returns
        # milliseconds. Getting this wrong makes the client refresh in a hot
        # loop or, worse, keep using an expired token until connections drop.
        return token, expiry_ms / 1000.0

    return oauth_cb


def client_config(
    bootstrap_servers: str,
    *,
    sasl_iam: bool = False,
    region: str = "",
) -> dict:
    """Base client config shared by the producer and the consumer."""
    config: dict = {"bootstrap.servers": bootstrap_servers}

    if sasl_iam:
        if not region:
            raise ValueError("kafka_sasl_iam requires aws_region to sign tokens")
        config.update(
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": "OAUTHBEARER",
                "oauth_cb": _oauth_token_provider(region),
            }
        )
        log.info("kafka: MSK IAM auth enabled (region=%s)", region)

    return config
