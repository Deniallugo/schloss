import asyncio

import pytest


def pytest_collection_modifyitems(items):
    for item in items:
        if asyncio.iscoroutinefunction(item.function):
            item.add_marker('asyncio')


@pytest.fixture(scope="session")
def url(docker_ip, docker_services):
    """Ensure that HTTP service is up and responsive."""

    # `port_for` takes a container port and returns the corresponding host port
    port = docker_services.port_for("kafka", 9092)
    url = "{}:{}".format(docker_ip, port)
    print(f"url {url}")
    return url


# @pytest.fixture(scope='session')
# def url(docker_services):
#     docker_services.start('kafka')
#     print(f"Docker {docker_services.docker_ip}")
#
#     return f'{docker_services.docker_ip}:{9092}'
