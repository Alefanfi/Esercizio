package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/rpc"
	"os"
)

func main() {

	//richiamo la routine per avviare il pub

	go connessionePub()

	for{}

}

//connessione con il pub

func connessionePub() {

	var msg Message

	fmt.Println("Launching pub...")

	for {

		//inserire i dati del messaggio

		reader := bufio.NewScanner(os.Stdin)

		fmt.Printf("Inserire i dati da inviare:")

		reader.Scan()

		text := reader.Text()

		msg.Data = text

		//Inserire il topic del messaggio

		reader2 := bufio.NewScanner(os.Stdin)

		fmt.Printf("Inserire il topic:")

		reader2.Scan()

		text2 := reader2.Text()

		msg.Topic = text2

		//Marshalling dei parametri

		s, err := json.Marshal(&msg)
		if err != nil {
			log.Fatal(err)
		}

		//fmt.Printf("ProvaPub: %+v\n", string(s))

		//mms := string(s[:])

		client, err := rpc.Dial("tcp", "localhost:8080")
		if err != nil {
			log.Fatal("dialing:", err)
		}

		if err != nil {
			log.Fatal(err)
		}

		var reply string
		divCall := client.Go("Listener.RiceviMsg", s, &reply, nil)
		replyCall := <-divCall.Done // will be equal to divCall

		if replyCall == nil {

			log.Fatal(replyCall)
		}

		fmt.Printf("Messaggio ricevuto : %+v\n", reply)

		client.Close()

	}

}
