#alive.go

An experimental client for sending keepalive messages to a Sensu server.

## Installation
Modify client.json and rabbitmq.json according to the environment

    go get github.com/streadway/amqp
    
    (go run alive.go)
    
    go build alive.go
    
## Todo
Split into packages