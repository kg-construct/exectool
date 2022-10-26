# bench-excutor

Bench-executor is a simple tool to execute benchmarks on tools which are running
in Docker.

## Installation

**Ubuntu 22.04 LTS**

Ubuntu 22.04 LTS or later is required because this is the first LTS version
with default CGroupFSv2 support. This tool will not function when not using the
GroupFSv2 SystemD driver in Docker! Patches are welcome to support other 
CGroupFS configurations.

You can check the CGroupFS driver with `docker info`:

```
Cgroup Driver: systemd
Cgroup Version: 2
```

1. Install dependencies

```
sudo apt install zlib1g zlib1g-dev libpq-dev libjpeg-dev python3-pip
```

```
pip install --user -r requirements.txt
```

2. Install Docker

```
# Install docker packages
sudo apt install docker.io

# Add user to docker group
sudo groupadd docker
sudo usermod -aG docker $USER
```

## License

Licensed under the GPLv3 license
Copyright (c) by Dylan Van Assche (2022)

