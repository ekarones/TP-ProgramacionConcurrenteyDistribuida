package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"sync"
)

func main() {
	// Abre el archivo de entrada
	archivo, err := os.Open("./positivos_covid.csv")
	if err != nil {
		panic(err)
	}
	defer archivo.Close()

	// Crea un canal para enviar datos desde el mapeador al reducidor
	canalDatos := make(chan string, 100)

	// Crea un mapa para almacenar los recuentos por departamento y provincia
	recuentos := make(map[string]int)

	// Crea un WaitGroup para esperar a que todas las goroutines finalicen
	var wg sync.WaitGroup

	// Función para el mapeo de datos
	mapeador := func(linea []string) {
		// Verifica si hay suficientes campos en la línea
		if len(linea) >= 3 {
			depto := linea[1]
			prov := linea[2]
			clave := fmt.Sprintf("%s - %s", depto, prov)

			// Envía el dato al canal
			canalDatos <- clave
		}
		wg.Done()
	}

	// Función para la reducción de datos
	reducidor := func() {
		for clave := range canalDatos {
			// Incrementa el contador para
			//la combinación de departamento y provincia
			recuentos[clave]++
		}
	}

	// Escanea el archivo CSV y realiza el mapeo
	lectorCSV := csv.NewReader(archivo)
	lectorCSV.Comma = ';' // Establece el punto y coma como delimitador
	for {
		linea, err := lectorCSV.Read()
		if err != nil {
			break
		}
		wg.Add(1)
		go mapeador(linea)
	}

	// Inicia el proceso de reducción
	go reducidor()

	// Espera a que todas las goroutines de mapeo finalicen
	wg.Wait()

	// Cierra el canal de datos una vez que las goroutines de mapeo hayan terminado
	close(canalDatos)

	// Espera a que la goroutine de reducción finalice
	wg.Wait()

	// Imprime los recuentos por departamento y provincia
	for clave, valor := range recuentos {
		fmt.Printf("%s: %d\n", clave, valor)
	}
}
