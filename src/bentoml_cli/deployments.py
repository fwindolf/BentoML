from __future__ import annotations

import logging
import json
from time import sleep
import attr
import yaml
import click
import typing as t
from rich.table import Table
from rich.syntax import Syntax

from bentoml._internal.yatai_rest_api_client.schemas import (
    CreateDeploymentTargetSchema,
    DeploymentStatus,
    CreateDeploymentSchema,
    DeploymentTargetConfig,
    DeploymentTargetHPAConf,
    DeploymentTargetResourceItem,
    DeploymentTargetResources,
    DeploymentTargetRunnerConfig,
    DeploymentTargetType,
    LabelItemSchema,
    UpdateDeploymentSchema,
)

logger = logging.getLogger("bentoml")


def parse_config(stem: str, config: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
    return {
        k.removeprefix(stem if stem.endswith(".") else stem + "."): arg
        for k, arg in config.items()
        if k.startswith(stem)
    }


def set_config(obj: object, key: str, value: t.Any):
    _obj = obj
    key_path = key.split(".")
    for field in key_path[:-1]:
        if not hasattr(_obj, field):
            raise AttributeError(f"Can not set {key} in object ({type(obj)})")

        _obj = getattr(_obj, field)

    setattr(_obj, key_path[-1], value)


def add_deployments_command(cli: click.Group) -> None:
    from bentoml_cli.utils import BentoMLCommandGroup
    from bentoml._internal.utils import rich_console as console
    from bentoml.exceptions import CLIException, YataiRESTApiClientError
    from bentoml._internal.yatai_rest_api_client.yatai import YataiRESTApiClient
    from bentoml._internal.yatai_rest_api_client.config import get_current_context

    try:
        ctx = get_current_context()
        yatai_rest_client = YataiRESTApiClient(ctx.endpoint, ctx.api_token)
    except YataiRESTApiClientError:
        raise CLIException("not logged in to yatai")

    @cli.group(name="deployments", cls=BentoMLCommandGroup)
    def deployments_cli():
        """Yatai Subcommands Groups"""

    @deployments_cli.command()
    @click.option(
        "-c", "--cluster", type=click.STRING, default="default", help="Yatai cluster"
    )
    @click.option(
        "-o",
        "--output",
        type=click.Choice(["json", "yaml", "table"]),
        default="console",
        help="Output of the request (json, yaml, table)",
    )
    @click.option(
        "-n",
        "--count",
        type=click.INT,
        default=None,
        help="Number of deployments to show",
    )
    @click.option(
        "--start",
        type=click.INT,
        default=None,
        help="Offset to start showing deployments from",
    )
    def list(  # type: ignore (not accessed)
        cluster: str,
        output: str,
        count: t.Optional[int],
        start: t.Optional[int],
    ) -> None:
        """Get a list of deployments from Yatai server.

        \b
        bentoml deployments list
        bentoml deployments list --cluster test
        bentoml deployments list --output=json
        bentoml deployments list --count=10 --start=0
        """
        deployments = yatai_rest_client.list_deployments(
            cluster_name=cluster, count=count, start=start
        )

        res = [
            {
                "name": deployment.name,
                "status": str(deployment.status.value),
                "url": deployment.urls[0] if deployment.urls else "",
                "created_at": deployment.created_at.astimezone().strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "updated_at": deployment.updated_at.astimezone().strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                if deployment.updated_at
                else "",
            }
            for deployment in sorted(
                deployments.items,
                key=lambda d: d.updated_at or d.created_at,
                reverse=True,
            )
        ]

        if output == "json":
            info = json.dumps(res, indent=2)
            console.print_json(info)
        elif output == "yaml":
            info = yaml.safe_dump(res, indent=2)
            console.print(Syntax(info, "yaml"))  # type: ignore
        else:
            table = Table(box=None)
            table.add_column("Name")
            table.add_column("Status")
            table.add_column("URL")
            table.add_column("Created At")
            table.add_column("Updated At")
            for deployment in res:
                table.add_row(
                    deployment["name"],
                    deployment["status"],
                    deployment["url"],
                    deployment["created_at"],
                    deployment["updated_at"],
                )
            console.print(table)

    @click.argument("name", type=click.STRING)
    @click.option(
        "-c", "--cluster", type=click.STRING, default="default", help="Yatai cluster"
    )
    @click.option(
        "-n", "--namespace", type=click.STRING, default="yatai", help="k8s namespace"
    )
    @click.option(
        "-o",
        "--output",
        type=click.Choice(["json", "yaml"]),
        default="yaml",
        help="Output of the request (json, yaml)",
    )
    def get(cluster: str, name: str, namespace: str, output: str) -> None:  # type: ignore (not accessed)
        """Get details for a deployment on Yatai.

        \b
        bentoml deployments get iris_classifier
        bentoml deployments get iris_classifier --cluster test
        bentoml deployments get iris_classifier --namespace yatai-iris
        bentoml deployments get iris_classifier --output=json
        """
        deployment = yatai_rest_client.get_deployment(
            cluster_name=cluster, deployment_name=name, kube_namespace=namespace
        )
        res = attr.asdict(deployment)
        if output == "json":
            info = json.dumps(res, indent=2)
            console.print_json(info)
        else:
            info = yaml.safe_dump(res, indent=2)
            console.print(Syntax(info, "yaml"))  # type: ignore

    @click.argument("name", type=click.STRING)
    @click.option(
        "-c", "--cluster", type=click.STRING, default="default", help="Yatai cluster"
    )
    @click.option(
        "-n", "--namespace", type=click.STRING, default="yatai", help="k8s namespace"
    )
    @click.option(
        "-b", "--block", type=click.BOOL, default=False, help="Wait until terminated"
    )
    def terminate(cluster: str, name: str, namespace: str, block: bool) -> None:  # type: ignore (not accessed)
        """Terminate a deployment on Yatai.

        \b
        bentoml deployments terminate iris_classifier
        bentoml deployments terminate iris_classifier --cluster test
        bentoml deployments terminate iris_classifier --namespace yatai-iris
        """
        try:
            yatai_rest_client.terminate_deployment(
                cluster_name=cluster, deployment_name=name, kube_namespace=namespace
            )
        except Exception as e:
            console.print(f"Could not terminate deployment: {e}")

        if not block:
            return

        retries = 0
        while True:
            deployment = yatai_rest_client.get_deployment(
                cluster_name=cluster, deployment_name=name, kube_namespace=namespace
            )
            if deployment.status == DeploymentStatus.NONDEPLOYED:
                console.print(f"Deployment '{name}' terminated.")
                break

            if retries >= 30:
                console.print(
                    f"Timed out waiting for deployment '{name}' to terminate."
                )
                break

            sleep(0.33)
            retries += 1

    @click.argument("name", type=click.STRING)
    @click.option(
        "-c", "--cluster", type=click.STRING, default="default", help="Yatai cluster"
    )
    @click.option(
        "-n", "--namespace", type=click.STRING, default="yatai", help="k8s namespace"
    )
    def delete(cluster: str, name: str, namespace: str) -> None:  # type: ignore (not accessed)
        """Delete a terminated deployment on Yatai.

        \b
        bentoml deployments delete iris_classifier
        bentoml deployments delete iris_classifier --cluster test
        bentoml deployments delete iris_classifier --namespace yatai-iris
        """
        try:
            yatai_rest_client.delete_deployment(
                cluster_name=cluster, deployment_name=name, kube_namespace=namespace
            )
        except Exception as e:
            console.print(f"Could not delete deployment: {e}")

    @click.argument(
        "name",
        type=click.STRING,
        help="Name of the deployment. Must be unique for this cluster and namespace",
    )
    @click.argument(
        "bento_repository",
        type=click.STRING,
        help="Bento repository that contains the bento",
    )
    @click.argument(
        "bento",
        type=click.STRING,
        help="Version of the bento to deploy. Can not be latest.",
    )
    @click.option(
        "-c", "--cluster", type=click.STRING, default="default", help="Yatai cluster"
    )
    @click.option(
        "-n", "--namespace", type=click.STRING, default="yatai", help="k8s namespace"
    )
    @click.option("-l", "--label", type=click.STRING, help="labels like 'foo:bar'")
    @click.option(
        "--do-not-deploy", type=click.BOOL, default=False, help="Do not deploy"
    )
    def create(  # type: ignore (not accessed)
        cluster: str,
        name: str,
        namespace: str,
        bento_repository: str,
        bento: str,
        description: str,
        label: str,
        do_not_deploy: bool,
        **kwargs,  # type: ignore
    ) -> None:
        """Create a new deployment on Yatai.
        
        \b
        bentoml deployments create iris_classifier_a iris_classifier qojf5xauugwqtgxi
        bentoml deployments create iris_classifier_a iris_classifier qojf5xauugwqtgxi \\
            --cluster test \\
            --namespace yatai-iris \\
            --description "Iris Classifier" \\
            --label "group:A" \\
            --do-not-deploy \\
            --config.resources.requests.cpu 1000m \\
            --config.resources.requests.memory 512Mi \\
            --config.resources.requests.gpu 1 \\
            --config.resources.limits.cpu 1500m \\
            --config.resources.limits.memory 1024Mi \\
            --config.resources.limits.gpu 1 \\
            --config.hpa_conf.cpu 1000m \\
            --config.hpa_conf.memory 512Mi \\
            --config.hpa_conf.qps 100 \\
            --config.hpa_conf.min_replicas 1 \\
            --config.hpa_conf.max_replicas 10 \\
            --config.env DEVELOPMENT=1
            --config.env LOG_LEVEL=DEBUG
            --config.runners.iris_runner_A.resources... \\
            --config.runners.iris_runner_A.hpa_conf... \\
            --config.runners.iris_runner_A.env... \\
            --config.runners.iris_runner_A.resources.requests.memory 512Mi \\
            --config.enable_ingress 1
        """
        config = parse_config("config", kwargs)  # type: ignore
        resource_requests = parse_config("config.resources.requests", kwargs)  # type: ignore
        resource_limits = parse_config("config.resources.limits", kwargs)  # type: ignore
        hpa_conf = parse_config("config.hpa_conf", kwargs)  # type: ignore
        envs = parse_config("config.env", kwargs)  # type: ignore

        runners = {
            runner.split(".")[0]
            for runner in parse_config("config.runners", kwargs).keys()
        }
        runner_configs = {}
        for runner in runners:
            config_name = f"config.runners.{runner}"
            runner_envs = parse_config(f"{config_name}.env", kwargs)  # type: ignore
            runner_hpa_conf = parse_config(f"{config_name}.hpa_conf", kwargs)  # type: ignore
            runner_requests = parse_config(f"{config_name}.resources.requests", kwargs)  # type: ignore
            runner_limits = parse_config(f"{config_name}.resources.limits", kwargs)  # type: ignore

            runner_config = DeploymentTargetRunnerConfig(
                envs=[LabelItemSchema(*env.split("=")) for env in runner_envs],
                hpa_conf=DeploymentTargetHPAConf(**runner_hpa_conf),
                resources=DeploymentTargetResources(
                    DeploymentTargetResourceItem(**runner_requests),
                    DeploymentTargetResourceItem(**runner_limits),
                ),
            )
            runner_configs[runner] = runner_config

        target_schema = CreateDeploymentTargetSchema(
            type=DeploymentTargetType.STABLE,
            bento=bento,
            bento_repository=bento_repository,
            config=DeploymentTargetConfig(
                kubeResourceUid="",
                kubeResourceVersion="",
                resources=DeploymentTargetResources(
                    DeploymentTargetResourceItem(**resource_requests),
                    DeploymentTargetResourceItem(**resource_limits),
                ),
                hpa_conf=DeploymentTargetHPAConf(**hpa_conf),
                envs=[LabelItemSchema(*env.split("=")) for env in envs] or None,
                runners=runner_configs,
                enable_ingress=config["enable_ingress"],
            ),
            canary_rules=[],
        )

        schema = CreateDeploymentSchema(
            name=name,
            description=description,
            kube_namespace=namespace,
            labels=LabelItemSchema(*label.split(":")),
            targets=[target_schema],
            do_not_deploy=do_not_deploy,
        )
        yatai_rest_client.create_deployment(cluster, schema)

    @click.argument(
        "name",
        type=click.STRING,
        help="Name of the deployment. Must be unique for this cluster and namespace",
    )
    @click.argument(
        "bento",
        type=click.STRING,
        help="Version of the bento to deploy. Can not be latest.",
    )
    @click.option("-l", "--label", type=click.STRING, help="labels like 'foo:bar'")
    @click.option(
        "-n", "--namespace", type=click.STRING, default="yatai", help="k8s namespace"
    )
    @click.option(
        "-c", "--cluster", type=click.STRING, default="default", help="Yatai cluster"
    )
    def update(  # type: ignore (not accessed)
        cluster: str,
        name: str,
        namespace: str,
        bento: t.Optional[str],
        description: t.Optional[str],
        do_not_deploy: bool,
        label: t.Optional[str] = None,
        **kwargs,  # type: ignore
    ) -> None:
        """Update a deployment on Yatai to the specified bento
        
        bentoml deployments update iris_classifier_a qojf5xauugwqtgxi
        bentoml deployments update iris_classifier_a qojf5xauugwqtgxi \\
            --cluster test \\
            --namespace yatai-iris \\
            --description "Iris Classifier" \\
            --label "group:A" \\
            --do-not-deploy \\
            --config.resources.requests.cpu 1000m \\
            --config.resources.requests.memory 512Mi \\
            --config.resources.requests.gpu 1 \\
            --config.resources.limits.cpu 1500m \\
            --config.resources.limits.memory 1024Mi \\
            --config.resources.limits.gpu 1 \\
            --config.hpa_conf.cpu 1000m \\
            --config.hpa_conf.memory 512Mi \\
            --config.hpa_conf.qps 100 \\
            --config.hpa_conf.min_replicas 1 \\
            --config.hpa_conf.max_replicas 10 \\
            --config.env DEVELOPMENT=1
            --config.env LOG_LEVEL=DEBUG
            --config.runners.iris_runner_A.resources... \\
            --config.runners.iris_runner_A.hpa_conf... \\
            --config.runners.iris_runner_A.env... \\
            --config.runners.iris_runner_A.resources.requests.memory 512Mi \\
            --config.enable_ingress 1
        """
        deployment = yatai_rest_client.get_deployment(
            cluster_name=cluster, deployment_name=name, kube_namespace=namespace
        )

        target = deployment.latest_revision.targets[0]
        update_target = CreateDeploymentTargetSchema(
            bento_repository=target.bento.repository.name,
            bento=bento or target.bento.name,
            canary_rules=target.canary_rules,
            config=target.config,
            type=DeploymentTargetType.STABLE,
        )
        for key, arg in kwargs.items():  # type: ignore
            if not key.startswith("config"):
                continue

            set_config(update_target.config, key.removesuffix("config."), arg)

        yatai_rest_client.update_deployment(
            cluster_name=cluster,
            deployment_name=name,
            kube_namespace=namespace,
            req=UpdateDeploymentSchema(
                description=description,
                labels=LabelItemSchema(*label.split(":")) if label else None,
                targets=[update_target],
                do_not_deploy=do_not_deploy,
            ),
        )
