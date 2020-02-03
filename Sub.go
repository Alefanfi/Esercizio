package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

var IP string

//rcp go che mi permette di ricevere i messaggi dalla coda

func (l *Listener) Ricevi(line []byte, reply *string) error {

	var m Message

	//faccio l'unmarshalling dei parametri

	err := json.Unmarshal(line, &m)
	if err != nil {
		log.Fatal(err)
	}

	// Risposta ricevuta dal server

	fmt.Printf("Messaggio ricevuto dal server: %+v\n", m)

	*reply = "ACK"

	return nil
}

func main() {

	//mi produco un seed da cui poi andare a generare le altre porte

	rand.Seed(time.Now().UTC().UnixNano())

	fmt.Println("Launching sub...")

	// massimo e minimo valore della porta

	max := 60000

	min := 50000

	port := rand.Intn(max-min) + min

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		os.Stderr.WriteString("Oops: " + err.Error() + "\n")
		os.Exit(1)
	}

	for _, a := range addrs {

		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {

			if ipnet.IP.To4() != nil {

				IP = ipnet.IP.String()

			}
		}
	}

	IP = "127.0.0.1"

	//richiamo la routine per avviare il sub

	go connessioneSub(port)

	//richiamo la routine per ricevere i messaggi dalla coda

	go riceviServer(port)

	for {
	}

}

//inizzializza la connessione con il sub

func connessioneSub(port int) {

	var comando int

	var text string

	for {

		//lista comandi

		fmt.Println("1.mostra lista\n" + "2.sottoscrivi topic\n" + "3.rimuovi sottoscrizione\n" + "Inserire un comando: ")

		// read in input from stdin

		_, err := fmt.Scanf("%d\n", &comando)
		if err != nil {
			log.Fatal(err)
		}

		switch comando {

		case 1:

			fmt.Println("Comando 1: mostra lista dei messaggi")

			client, err := rpc.Dial("tcp", "localhost:1234")
			if err != nil {
				log.Fatal("dialing:", err)
			}

			var reply []Message

			divCall := client.Go("Listener.ListaMsg", []byte("list"), &reply, nil)
			replyCall := <-divCall.Done //Will be equal to divCall

			if replyCall == nil {

				log.Fatal(replyCall)
			}

			fmt.Printf("Lista dei messaggi: %+v\n", reply[:])

			client.Close()

		case 2:

			fmt.Println("Comando 2: sottoscrizione a un topic")

			fmt.Printf("Inserire il topic a cui vuoi sottoscriverti: ")

			reader := bufio.NewScanner(os.Stdin)

			reader.Scan()

			text = reader.Text() + " " + IP + " " + strconv.Itoa(port)

			client, err := rpc.Dial("tcp", "localhost:2345")
			if err != nil {
				log.Fatal("dialing:", err)
			}

			var reply string
			divCall := client.Go("Listener.Subscribe", text, &reply, nil)
			replyCall := <-divCall.Done //Will be equal to divCall

			if replyCall == nil {

				log.Fatal(replyCall)
			}

			fmt.Println("Messaggio ricevuto : ", reply)

			client.Close()

		case 3:

			fmt.Println("Comando 3: rimozione di un topic")

			fmt.Printf("Inserire il topic da cui rimuovere la sottoscrizione: ")

			reader := bufio.NewScanner(os.Stdin)

			reader.Scan()

			text = reader.Text() + " " + IP + " " + strconv.Itoa(port)

			client, err := rpc.Dial("tcp", "localhost:3456")
			if err != nil {
				log.Fatal("dialing:", err)
			}

			var reply string

			divCall := client.Go("Listener.RimuoviSotTopic", text, &reply, nil)
			replyCall := <-divCall.Done //Will be equal to divCall

			if replyCall == nil {

				log.Fatal(replyCall)
			}

			fmt.Println("Messaggio ricevuto : ", reply)

			client.Close()
		}

	}

}

//funzione che inizializza la connessione con il server così che il sub possa ricevere i messaggi in coda

func riceviServer(port int) {

	addy, err := net.ResolveTCPAddr("tcp", "0.0.0.0:"+strconv.Itoa(port))
	if err != nil {
		log.Fatal(err)
	}

	l, e := net.ListenTCP("tcp", addy)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	listener := new(Listener)

	rpc.Register(listener)

	for {

		rpc.Accept(l)

	}

}
