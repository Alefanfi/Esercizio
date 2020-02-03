package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

//struttura dei topic

type Topic struct {

	//argomento del messaggio
	topic string
	//IP []byte
	IP []string
	//numero di porta
	port []int
}

var msgs *[]Message
var topics []Topic
var semantica int
var timeoutUtente int

//Lista che fornisce l'elenco dei messaggi che sono presenti nella coda

func (l *Listener) ListaMsg(line []byte, list *[]Message) error {

	//fmt.Println("Ho ricevuto: " + string(line))
	fmt.Printf("Stampo lista messaggi: %+v\n", (*msgs)[:])

	switch semantica {

	case 1:

		//Semantica at least once: stampo tutti i messaggi presenti all'interno della coda

		*list = *msgs

	case 2:

		//Semantica timeout-based: stampo tutti i messaggi presenti all'interno della coda
		// che hanno il valore visibility = 0

		var newlist []Message

		for i := 0; i < len(*msgs); i++ {

			if (*msgs)[i].Visibility == 0 {

				newlist = append(newlist, (*msgs)[i])

			}

		}

		*list = newlist

	}

	return nil
}

//funzione che riceve i messaggi da parte dei pub

func (l *Listener) RiceviMsg(line []byte, reply *string) error {

	var m Message
	var topic Topic

	//Unmarshal dei dati inseriti

	err := json.Unmarshal([]byte(line), &m)
	if err != nil {
		log.Fatal(err)
	}

	//imposto la visibilità di default a 0

	m.Visibility = 0

	//fmt.Printf("ProvaPub: %+v\n", m)

	msgs = inserisci(*msgs, &m)

	topic.topic = m.Topic

	flag := 0

	//Controllo che il topic non sia già presente

	for i := 0; i < len(topics); i++ {
		if topics[i].topic == topic.topic {
			flag = 1
			break
		}
	}

	if flag == 0 {
		topics = append(topics, topic)
	}

	*reply = "ACK"

	//in base al valore della semantica che è stato aggiunto dall'utente eseguirà
	//o la routine per la semantica at leasta once o la routine per la semantica timeout based

	switch semantica {

	case 1:

		//Semantica at least once

		go invioSubAtLeastOnce()

	case 2:

		//Semantica timeout-based

		go invioSubTimeoutBased()

	}

	return nil
}

//funzione che permette la sottoscrizione dei sub a un particolare topic

func (l *Listener) Subscribe(line string, res *string) error {

	// output message received

	fmt.Print("Messaggio ricevuto del subscribe: \n", line)

	// check to see which comand was received

	splits := strings.Split(line, " ")

	fmt.Printf("Stampo lista topic: %+v\n", topics[:])

	//fmt.Printf("Stampo splits: %+v\n", splits[:])

	flag := 0

	remoteIp := strings.Split(splits[1], ":")[0]

	port, err := strconv.Atoi(splits[2])
	if err != nil {
		// handle error
		fmt.Println(err)
		os.Exit(2)
	}

	// controllo che esista il topic

	for i := 0; i < len(topics); i++ {

		//fmt.Println("Topic received \n: " + splits[0])

		if topics[i].topic == splits[0] {

			flag = 1

			//controllo che non sia già sottoscritto

			for j := 0; j < len(topics[i].IP); j++ {

				if topics[i].IP[j] == remoteIp && topics[i].port[j] == port {

					flag = 2
					break
				}

			}

			if flag == 1 {

				topics[i].IP = append(topics[i].IP, remoteIp)

				topics[i].port = append(topics[i].port, port)
			}

			break

		}

	}

	//invia la risposta al sub

	if flag == 0 {

		*res = "Topic not found!"

	} else if flag == 1 {

		*res = "Subscribed to " + splits[0]

	} else {

		*res = "You are already subscribed"

	}

	switch semantica {

	case 1:

		//invio dei messaggi dalla cosa al rispettivo sub con semantica at least once

		go invioSubAtLeastOnce()

	case 2:

		//invio dei messaggi dalla cosa al rispettivo sub con semantica timeout-based

		go invioSubTimeoutBased()

	}

	return nil
}

//rpc go che permette di rimuovere la sottoscrizione di un sub a un topic

func (l *Listener) RimuoviSotTopic(line string, res *string) error {

	//fmt.Println("Ho ricevuto: " + string(line))

	splits := strings.Split(line, " ")

	fmt.Printf("Stampo lista topic: %+v\n", topics[:])

	//fmt.Printf("Stampo splits: %+v\n", splits[:])

	flag := 0
	remoteIp := strings.Split(splits[1], ":")[0]
	port, err := strconv.Atoi(splits[2])

	if err != nil {
		// handle error
		fmt.Println(err)
		os.Exit(2)
	}

	var i int
	var j int

	// controllo che esista il topic

	for i = 0; i < len(topics); i++ {

		fmt.Println("Topic ricevuto : " + splits[0])

		if topics[i].topic == splits[0] {

			flag = 1

			//controllo che non sia già sottoscritto

			for j = 0; j < len(topics[i].IP); j++ {

				if topics[i].IP[j] == remoteIp && topics[i].port[j] == port {

					flag = 2
					break
				}

			}

			if flag == 2 {

				copy(topics[i].IP[j:], topics[i].IP[j+1:])
				topics[i].IP = topics[i].IP[:len(topics[i].IP)-1]

				copy(topics[i].port[j:], topics[i].port[j+1:])
				topics[i].port = topics[i].port[:len(topics[i].port)-1]

			}

			break

		}

	}

	//in base al valore del flag invio una risposta al sub

	if flag == 0 {

		*res = "Topic non trovato!"

	} else if flag == 2 {

		*res = "Eliminata la sottoscrizione al topic  " + splits[0]

	} else {

		*res = "Non eri più sottoscritto"

	}

	return nil
}

func main() {

	var topics []Topic

	//richiamo la routine per inizializzare la connessione con il sub

	go initPub(msgs, &topics)

	//richiamo la routine per inizializzare la connessione con i subs

	go initSub()

	//richiamo le routine per rimuovere una sottoscrizione e stampare la lista dei messaggi

	go initRimuovi()

	go initList()

	for {
	}
}

//inizializzo la connessione con il pub

func initPub(queue *[]Message, topics *[]Topic) {

	msgs = creaCoda()

	//socket TCP

	fmt.Println("Launching server...")

	//definisco il tipo di semantica da utilizzare

	fmt.Println("1. at least once\n" +
		"2.timeout-based\n" +
		"Inserire il tipo di semantica da utilizzare: ")

	_, err := fmt.Scanf("%d\n", &semantica)
	if err != nil {
		log.Fatal(err)
	}

	switch semantica {

	case 1:

		fmt.Println("Hai scelto la semantica at-least-once")

		//richiedo il valore del timeout

		fmt.Println("Inserire il valore del timeout di ritrasmissione: ")
		_, err := fmt.Scanf("%d\n", &timeoutUtente)
		if err != nil {
			log.Fatal(err)
		}

	case 2:

		fmt.Println("Hai scelto la semantica timeout-based")

		//richiedo il valore del timeout

		fmt.Println("Inserire il valore del timeout di visibilità: ")
		_, err := fmt.Scanf("%d\n", &timeoutUtente)
		if err != nil {
			log.Fatal(err)
		}

	}

	// run loop forever

	for {

		addy, err := net.ResolveTCPAddr("tcp", "0.0.0.0:8080")
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
}

//funzione per inizializzare la connessione con il sub

func initSub() {

	addy, err := net.ResolveTCPAddr("tcp", "0.0.0.0:2345")
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

//inizializzo la connessione con il sub per ricevere la lista dei messaggi presenti nella coda

func initList() {

	addy, err := net.ResolveTCPAddr("tcp", "0.0.0.0:1234")
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

//inizializzo la conennessione per andare poi a rimuvore una sottoscrizione

func initRimuovi() {

	addy, err := net.ResolveTCPAddr("tcp", "0.0.0.0:3456")
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

//funzione che gestisce l'invio dei messaggi al sub con una semantica di tipo at least once

func invioSubAtLeastOnce() {

	var i int
	var k int
	var flag int
	//numero del messaggio corrente
	var currmess int
	//lunghezza della lista dei messaggi
	var lenlist int

	if len(*msgs) == 0 {
		return
	}

	if len(topics) == 0 {
		return
	}

	currmess = 0
	lenlist = len(*msgs)

	for k = 0; k < lenlist; k++ {

		// prendo il messaggio nella coda
		m := (*msgs)[currmess]
		topic := m.Topic
		flag = 0

		for i = 0; i < len(topics); i++ {
			if topic == topics[i].topic {
				break
			}
		}

		for j := 0; j < len(topics[i].IP); j++ {

			// Prova ad inviare il messaggio finchè non ci riesce
			for {

				client, err := rpc.Dial("tcp", string(topics[i].IP[j])+":"+strconv.Itoa(topics[i].port[j]))
				if err != nil {
					log.Fatal("dialing:", err)
					continue
				}

				s, err := json.Marshal(&m)
				if err != nil {
					log.Fatal(err)
					client.Close()
					continue
				}

				var reply string
				divCall := client.Go("Listener.Ricevi", s, &reply, nil)
				replyCall := <-divCall.Done //Will be equal to divCall

				if replyCall == nil {
					log.Fatal(replyCall)
					client.Close()
					continue
				}

				fmt.Println("Messaggio ricevuto :", reply)

				client.Close()
				flag++
				break
			}
		}

		if len(topics[i].IP) != 0 && flag == len(topics[i].IP) {

			//se il messsaggio è stato ricevuto correttamente rimuovilo

			*msgs = rimuovi(*msgs, currmess)

		} else {

			currmess++

		}

		//timeout della semantica at least once

		timeout := make(chan bool, 1)

		go func() {

			time.After(time.Duration(timeoutUtente) * time.Second)

			timeout <- true

		}()

	}

}

//funzione che gestisce l'invio dei messaggi al sub con una semantica di tipo timeout-based

func invioSubTimeoutBased() {

	var i int
	var k int
	var flag int
	//numero del messaggio corrente
	var currmess int
	//lunghezza della lista dei messaggi
	var lenlist int

	if len(*msgs) == 0 {
		return
	}

	if len(topics) == 0 {
		return
	}

	currmess = 0
	lenlist = len(*msgs)

	for k = 0; k < lenlist; k++ {

		// prendo il messaggio nella coda
		m := (*msgs)[currmess]
		topic := m.Topic
		flag = 0

		for i = 0; i < len(topics); i++ {
			if topic == topics[i].topic {
				break
			}
		}

		for j := 0; j < len(topics[i].IP); j++ {

			// Prova ad inviare il messaggio finchè non ci riesce
			for {

				m.Visibility = 1

				client, err := rpc.Dial("tcp", string(topics[i].IP[j])+":"+strconv.Itoa(topics[i].port[j]))
				if err != nil {
					fmt.Println("dialing:" + err.Error())
					m.Visibility = 0
					break
				}

				//Eseguo il marshaling dei dati
				s, err := json.Marshal(&m)
				if err != nil {
					log.Fatal(err)
					client.Close()
					m.Visibility = 0
					break
				}

				var reply string
				divCall := client.Go("Listener.Ricevi", s, &reply, nil)
				replyCall := <-divCall.Done //Will be equal to divCall

				if replyCall == nil {
					fmt.Println(replyCall)
					client.Close()
					m.Visibility = 0
					break
				}

				fmt.Println("Messaggio ricevuto :", reply)

				client.Close()
				flag++

				*msgs = rimuovi(*msgs, currmess)

				//timeout

				timeout := make(chan bool, 1)

				go func() {

					time.After(time.Duration(timeoutUtente) * time.Second)

					timeout <- true

				}()

				break
			}

			if flag > 0 {

				break
			}
		}

		if flag == 0 {

			currmess++
		}

	}

}
