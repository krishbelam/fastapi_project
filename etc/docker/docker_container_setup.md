# Setting up the docker containers to work with this app

## RabbitMQ

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4-management
```

## MySQL

```bash
docker run --name my-mysql -e MYSQL_ROOT_PASSWORD=password -p 3306:3306 -d mysql:latest
```