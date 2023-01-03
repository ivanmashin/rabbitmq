package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func newConn() (*Connection, error) {
	connUrl := fmt.Sprintf(
		"amqp://%s:%s@%s:%s/",
		os.Getenv("USER"),
		os.Getenv("PASSWORD"),
		os.Getenv("HOST"),
		os.Getenv("PORT"),
	)

	conn, err := amqp.Dial(connUrl)
	if err != nil {
		return nil, err
	}

	connection := NewConnection(conn, func() (*amqp.Connection, error) {
		return amqp.Dial(connUrl)
	})
	if err != nil {
		return nil, err
	}

	return connection, nil
}

var (
	restartRabbitServerCommand          = os.Getenv("RESTART_COMMAND")
	restartRabbitServerCommandArguments = strings.Split(
		os.Getenv("RESTART_COMMAND_ARGS"), " ",
	)

	stopRabbitServerCommand          = os.Getenv("STOP_COMMAND")
	stopRabbitServerCommandArguments = strings.Split(
		os.Getenv("STOP_COMMAND_ARGS"), " ",
	)
)

func restartRabbitAndWaitReconnect(seconds int) error {
	cmd := exec.Command(restartRabbitServerCommand, restartRabbitServerCommandArguments...)
	err := cmd.Run()
	if err != nil {
		return err
	}

	// wait till restart
	time.Sleep(time.Duration(seconds) * time.Second)

	return nil
}

func TestConnection_Alive(t *testing.T) {
	conn, err := newConn()
	if err != nil {
		t.Error(err)
	}

	if !conn.IsAlive() {
		t.Error("connection is not alive")
	}

	err = restartRabbitAndWaitReconnect(7)
	if err != nil {
		t.Error(err)
	}

	if !conn.IsAlive() {
		t.Error("connection is not alive after restart")
	}
}
