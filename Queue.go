package main

import "fmt"

//Struttura messaggi scambiati

type Message struct {

	Data       string
	Topic      string
	Visibility int

}

type Listener int

//funzione per creare una nuova coda

func creaCoda() *[]Message {

	//creo una lista all'interno della quale memorizzare i messaggi in arrivo dal publish

	var lista []Message
	return &lista

}

//funzione per inserire i messaggi nella coda

func inserisci(lista []Message, m *Message) *[]Message {

	//una volta che mi arriva un messaggio dal pub lo inserisco all'interno della lista che avevo creato precedentemente

	fmt.Printf("Messaggio che arriva all'insert: %+v\n", m)

	lista = append(lista, *m)

	fmt.Printf("Stampo lista in inserisci: %+v\n", lista[:])

	return &lista

}

//funzione per rimuovere i messaggi nella coda dopo che il messaggio è stato consegnato a tutti i sub sottoscritti al topic

func rimuovi(lista []Message, i int) []Message {

	//controllo la lunghezza della lista prima di eliminare un messaggio

	if len(lista) <= 1 {
		lista = *creaCoda()
		return lista
	}

	//fmt.Printf("Sono in rimuovi\n")

	copy(lista[i:], lista[i+1:])

	lista = lista[:len(lista)-1]

	//stampo la nuova lista (con l'elemento cancellato)

	fmt.Printf("Stampo lista nella rimuovi: %+v\n", lista[:])

	return lista

}

