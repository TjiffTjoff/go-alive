package main

import (
  "fmt"
  "github.com/streadway/amqp"
  "time"
  "encoding/json"
  "os"
  "io/ioutil"
  "strconv"
  "strings"
)

// Structs to hold client configuration
type Client struct {
  Config  ClientConfig   `json:"client"`
}

type ClientConfig struct {
  Name          string    `json:"name"`
  Address       string    `json:"address"`
  Subscriptions []string  `json:"subscriptions"`
}

type Rabbitmq struct {
  Config  RabbitmqConfig   `json:"rabbitmq"`
}

type RabbitmqConfig struct {  
  Port          int       `json:"port"`
  Host          string    `json:"host"`
  User          string    `json:"user"`
  Password      string    `json:"password"`
  Vhost         string    `json:"vhost"`
}

// Struct with necessary information for keepalive event
type KeepAlive struct {
  Name          string    `json:"name"`
  Address       string    `json:"address"`
  Subscriptions []string  `json:"subscriptions"`
  Timestamp     int64     `json:"timestamp"`
}

func parseConfig() (ClientConfig, RabbitmqConfig, error) {
  clientJson, err := ioutil.ReadFile("client.json")
  if err != nil {
    return ClientConfig{}, RabbitmqConfig{}, err
  }
  
  rabbitmqJson, err := ioutil.ReadFile("rabbitmq.json")
  if err != nil {
    return ClientConfig{}, RabbitmqConfig{}, err
  }
  
  var client Client
  var rabbitmq Rabbitmq
  
  if err := json.Unmarshal(clientJson, &client); err != nil {
    return ClientConfig{}, RabbitmqConfig{}, err
  }
  
  if err := json.Unmarshal(rabbitmqJson, &rabbitmq); err != nil {
    return ClientConfig{}, RabbitmqConfig{}, err
  }
  
    return client.Config, rabbitmq.Config, nil
}
 
// Function to establish connection to amqp server
func connect(server string, port int, user string, password string, vhost string) (*amqp.Connection, error) {
  // Workaround to parse / in vhost name to %2F
  parsedVhost := strings.Replace(vhost, "/", "%2F", -1)

  // Create a uri string from arguments
  uri := "amqp://" + user + ":" + password + "@" + server + ":" + strconv.Itoa(port) + "/" + parsedVhost
  
  // Open a connection to the amqp server
  conn, err := amqp.Dial(uri)
  if err != nil {
    return nil, err
  }
  
  return conn, nil
}

// Function to open channel on the amqp connection
func channel(conn *amqp.Connection, msgtype string) (*amqp.Channel, error) {
  // Open a channel to communicate with the server
  channel, err := conn.Channel()
  if err != nil {
    return nil, err
  }

  // Declare the exchange to use when publishing
  if err := channel.ExchangeDeclare(
    msgtype, 
    "direct", 
    false, 
    false, 
    false, 
    false, 
    nil,
  ); err != nil {
    return nil, err
  }

  // Declare the queue to use when publishing
  channel.QueueDeclare(
    msgtype, 
    false, 
    true, 
    false, 
    false, 
    nil,
  )

  // Bind the queue to the exchange
  channel.QueueBind(
    msgtype, 
    "", 
    msgtype, 
    false, 
    nil,
  )
  
  return channel, nil
}

// Function to send keep alive message over specified channel
func sendKeepAlive(channel *amqp.Channel, client ClientConfig) error {
  // Create a keepalive struct to send to server
  body := &KeepAlive{
    Name: client.Name,
    Address: client.Address,
    Subscriptions: client.Subscriptions,
    Timestamp: time.Now().Unix(),
  }

  // Parse the keepalive struct to json
  bodyJson, err := json.Marshal(body) 
  if err != nil {
    return err
  }

  // Create the amqp message to publish
  msg := amqp.Publishing{
    ContentType: "application/octet-stream",
    DeliveryMode: amqp.Persistent,
    Priority: 0,
    Body: bodyJson,
  }

  // Publish message to amqp server
  if err := channel.Publish("keepalives", "", false, false, msg); err != nil {
    return err
  }
  
  // Returns nil as error if message was sent successfully
  return nil
}

func main() {

  client, rabbitmq, err := parseConfig()
  if err != nil {
    fmt.Printf("Configuration: %s \n", err)
    os.Exit(1)
  }
    
  // Establishing connection to amqp server
  //conn, err := connect("localhost", "5672", "sensu", "sensu")
  conn, err := connect(rabbitmq.Host, rabbitmq.Port, rabbitmq.User, rabbitmq.Password, rabbitmq.Vhost)
  if err != nil {
    fmt.Printf("Connection: %s \n", err)
    os.Exit(1)
  }
  
  // Opening a channel for keepalive messages
  keepAliveChannel, err := channel(conn, "keepalives")
  if err != nil {
    fmt.Printf("Channel: %s \n", err)
    os.Exit(1)
  }
  
  // Send keep alive every minute over keepalive channel
  for {
    fmt.Println("Sending keepalive")
    if err:= sendKeepAlive(keepAliveChannel, client); err != nil {
      fmt.Printf("Keepalive: %s \n", err)
      os.Exit(1)
    }
    time.Sleep(60 * time.Second)
  }
}
