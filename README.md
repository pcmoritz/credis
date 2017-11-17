# Chain Replicated Redis

## Setting up consul

To install, first download consul:

```
wget https://releases.hashicorp.com/consul/1.0.0/consul_1.0.0_linux_amd64.zip ; unzip consul_1.0.0_linux_amd64.zip
```

Set up a consul cluster:

Run the following on each machine, with a unique <node-name> and the <node-ip> of that node:

```
consul agent -server -bootstrap-expect=3 -data-dir=/tmp/consul -node=<node-name> -bind=<node-ip> -enable-script-checks=true -config-dir=.
```

Then run this command on one of the machines with the list of IP addresses of nodes in the cluster:

```
./consul join <node-ip> <node-ip> <node-ip> <node-ip>
```
