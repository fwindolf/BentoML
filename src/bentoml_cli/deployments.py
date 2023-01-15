from __future__ import annotations
from datetime import datetime
import enum
import json

import logging
from pathlib import Path
import attr
import cattr
import yaml
import click
import typing as t
from rich.table import Table
from rich.syntax import Syntax

from dateutil.parser import parse

from bentoml._internal.yatai_rest_api_client.schemas import (
    CreateDeploymentSchema,
    DeploymentTargetConfig,
    UpdateDeploymentSchema,
)

logger = logging.getLogger("bentoml")
converter = cattr.Converter()

time_format = "%Y-%m-%d %H:%M:%S.%f"


def datetime_decoder(datetime_str: t.Optional[str], _: t.Any) -> t.Optional[datetime]:
    if not datetime_str:
        return None
    return parse(datetime_str)


def datetime_encoder(time_obj: t.Optional[datetime]) -> t.Optional[str]:
    if not time_obj:
        return None
    return time_obj.strftime(time_format)


converter.register_structure_hook(datetime, datetime_decoder)
converter.register_unstructure_hook(datetime, datetime_encoder)


def datetime_hmr(value: t.Optional[datetime]) -> str:
    if value is None:
        return ""
    return value.astimezone().strftime("%Y-%m-%d %H:%M:%S")


def enum_hmr(value: enum.Enum) -> str:
    return value.value


def config_hmr(config: DeploymentTargetConfig) -> str:
    limits = config.resources.limits
    if limits is None:
        limits_str = "No Limits"
    else:
        limits_str = f"Limits: [{limits.cpu}, {limits.memory}]"

    requests = config.resources.requests
    if requests is None:
        requests_str = "No Requests"
    else:
        requests_str = f"Requests: [{requests.cpu}, {requests.memory}]"

    ingress_str = "Ingress: Enabled" if config.enable_ingress else "Ingress: Disabled"

    return f"{requests_str}, {limits_str}, {ingress_str}"


def serialize_values(inst: t.Any, a: t.Any, v: t.Any) -> str:
    if isinstance(v, datetime):
        return datetime_hmr(v)
    if isinstance(v, enum.Enum):
        return enum_hmr(v)

    return v


def parse_extra_args(args: t.List[str]) -> t.List[t.Tuple[str, str]]:
    split_args = [arg.split("=", maxsplit=1) for arg in args]
    args = [arg for args in split_args for arg in args]
    keys = [key.removeprefix("-").removeprefix("-") for key in args[0::2]]
    values = [val.strip('"').strip("'") for val in args[1::2]]
    return list(zip(keys, values))


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
        """Deployment Subcommands Groups"""

    @deployments_cli.command()
    @click.option(
        "-c", "--cluster", type=click.STRING, default="default", help="Yatai cluster"
    )
    @click.option(
        "-o",
        "--output",
        type=click.Choice(["json", "yaml", "table"]),
        default="table",
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
                "created_at": datetime_hmr(deployment.created_at),
                "updated_at": datetime_hmr(deployment.latest_revision.updated_at)
                if deployment.updated_at
                else "",
            }
            for deployment in sorted(
                deployments.items,
                key=lambda d: d.latest_revision.updated_at or d.created_at,
                reverse=True,
            )
        ]

        if output == "json":
            console.print_json(data=res)
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

    @deployments_cli.command()
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
        type=click.Choice(["json", "yaml", "summary"]),
        default="summary",
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
        res = attr.asdict(deployment, value_serializer=serialize_values)
        if output == "json":
            console.print_json(data=res)
        elif output == "yaml":
            info = yaml.safe_dump(res, indent=2)
            console.print(Syntax(info, "yaml"))  # type: ignore
        elif output == "summary":
            table = Table(box=None)
            table.add_column("Name")
            table.add_column("Status")
            table.add_column("Bento Repository")
            table.add_column("Bento Version")
            table.add_column("URLS")
            table.add_column("Created At")
            table.add_column("Updated At")
            table.add_column("ENVs")
            table.add_column("Config")

            targets = deployment.latest_revision.targets
            table.add_row(
                deployment.name,
                deployment.status.value,
                ", ".join([target.bento.repository.name for target in targets]),
                ", ".join([target.bento.name for target in targets]),
                ", ".join(deployment.urls),
                datetime_hmr(deployment.created_at),
                datetime_hmr(deployment.latest_revision.updated_at),
                ", ".join([str(target.config.envs) for target in targets]),
                ", ".join([config_hmr(target.config) for target in targets]),
            )

            console.print(table)

    @deployments_cli.command()
    @click.argument("name", type=click.STRING)
    @click.option(
        "-c", "--cluster", type=click.STRING, default="default", help="Yatai cluster"
    )
    @click.option(
        "-n", "--namespace", type=click.STRING, default="yatai", help="k8s namespace"
    )
    def terminate(cluster: str, name: str, namespace: str) -> None:  # type: ignore (not accessed)
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
            exit(1)

        console.print(f"Terminated '{name}'")

    @deployments_cli.command()
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

    @deployments_cli.command(
        context_settings=dict(
            allow_extra_args=True,
            ignore_unknown_options=True,
        )
    )
    @click.argument("name", type=click.STRING)
    @click.argument("bento_repository", type=click.STRING)
    @click.argument("bento", type=click.STRING)
    @click.option(
        "-c", "--cluster", type=click.STRING, default="default", help="Yatai cluster"
    )
    @click.option(
        "-n", "--namespace", type=click.STRING, default="yatai", help="k8s namespace"
    )
    @click.option(
        "--description", type=click.STRING, default=None, help="Deployment description"
    )
    @click.option(
        "-l",
        "--labels",
        type=click.STRING,
        help="Deployment label like 'foo:bar'",  # TODO: multiple
    )
    @click.option(
        "--do-not-deploy", type=click.BOOL, default=False, help="Do not deploy"
    )
    @click.option(
        "--config-json",
        type=click.STRING,
        default=None,
        help="Path to input json file that contains the deployment definition",
    )
    @click.option(
        "--config-yaml",
        type=click.STRING,
        default=None,
        help="Path to input yaml file that contains the deployment definition",
    )
    def create(  # type: ignore (not accessed)
        cluster: str,
        name: str,
        namespace: str,
        bento_repository: str,
        bento: str,
        description: t.Optional[str],
        labels: t.List[str],
        do_not_deploy: bool,
        config_json: t.Optional[str],
        config_yaml: t.Optional[str],
    ) -> None:
        """Create a new deployment on Yatai.

        \b
        bentoml deployments create iris_classifier_a iris_classifier qojf5xauugwqtgxi \\
            --config-json ./iris_classifier.json
        bentoml deployments create iris_classifier_a iris_classifier qojf5xauugwqtgxi \\
            --config-yaml ./iris_classifier.yaml
        """
        if config_json:
            config_path = Path(config_json).resolve()
            assert config_path.exists(), f"Config JSON not found at {config_path}"
            config = json.loads(config_path.read_text())
        elif config_yaml:
            config_path = Path(config_yaml).resolve()
            assert config_path.exists(), f"Config Yaml not found at {config_path}"
            config = yaml.load(config_path.read_bytes())
        else:
            raise CLIException("Missing deployment configuration")

        # TODO: I envision it like this:
        # You have a generic deployment configuration template and want to deploy bentos
        # with this configuration. So we put in the non-generic information into the
        # template configuration.
        # What's left here would be to make every parameter optional so we can also use
        # the complete configuration if need be.
        config["name"] = name
        config["kube_namespace"] = namespace
        config["description"] = description
        config["labels"] = labels
        config["do_not_deploy"] = do_not_deploy
        config["targets"][0]["bento"] = bento
        config["targets"][0]["bento_repository"] = bento_repository

        schema = converter.structure(config, CreateDeploymentSchema)
        try:
            yatai_rest_client.create_deployment(cluster, schema)
        except Exception as e:
            raise CLIException(f"Could not create deployment: {e}")

    @deployments_cli.command(
        context_settings=dict(
            allow_extra_args=True,
            ignore_unknown_options=True,
        )
    )
    @click.argument("name", type=click.STRING)
    @click.argument("bento", type=click.STRING)
    @click.option(
        "-n", "--namespace", type=click.STRING, default="yatai", help="k8s namespace"
    )
    @click.option(
        "-c", "--cluster", type=click.STRING, default="default", help="Yatai cluster"
    )
    @click.option(
        "--description", type=click.STRING, default=None, help="Deployment description"
    )
    @click.option(
        "-l",
        "--labels",
        type=click.STRING,
        help="Deployment label like 'foo:bar'",  # TODO: multiple
    )
    @click.option(
        "--do-not-deploy", type=click.BOOL, default=False, help="Do not deploy"
    )
    @click.option(
        "--config-json",
        type=click.STRING,
        default=None,
        help="Path to input json file that contains the deployment definition",
    )
    @click.option(
        "--config-yaml",
        type=click.STRING,
        default=None,
        help="Path to input yaml file that contains the deployment definition",
    )
    @click.pass_context
    def update(  # type: ignore (not accessed)
        cluster: str,
        name: str,
        namespace: str,
        bento: t.Optional[str],
        description: t.Optional[str],
        labels: t.List[str],
        do_not_deploy: bool,
        config_json: t.Optional[str],
        config_yaml: t.Optional[str],
    ) -> None:
        """Update a deployment on Yatai to the specified bento

        bentoml deployments update iris_classifier_a qojf5xauugwqtgxi \\
            --config-json ./iris_classifier_a.json
        bentoml deployments update iris_classifier_a qojf5xauugwqtgxi \\
            --config-yaml ./iris_classifier_a.yaml
        """
        deployment = yatai_rest_client.get_deployment(
            cluster_name=cluster, deployment_name=name, kube_namespace=namespace
        )

        if config_json:
            config_path = Path(config_json).resolve()
            assert config_path.exists(), f"Config JSON not found at {config_path}"
            config = json.loads(config_path.read_text())
        elif config_yaml:
            config_path = Path(config_yaml).resolve()
            assert config_path.exists(), f"Config Yaml not found at {config_path}"
            config = yaml.load(config_path.read_bytes())
        else:
            raise CLIException("Missing deployment configuration")

        config["description"] = description
        config["bento"] = bento
        config["labels"] = labels
        config["do_not_deploy"] = do_not_deploy

        current_config = converter.unstructure(deployment, UpdateDeploymentSchema)
        config = current_config | config

        schema = converter.structure(config, UpdateDeploymentSchema)

        yatai_rest_client.update_deployment(
            cluster_name=cluster,
            deployment_name=name,
            kube_namespace=namespace,
            req=schema,
        )
