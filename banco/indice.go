package banco

import (
	"encoding/binary"
	"errors"
	"log"
	"os"
)

//https://golang.org/pkg/os/#File.Seek
const inicioArquivo = 0
const finalArquivo = 2

var duplicado error = errors.New("chave duplicada")

type IndicePrimario struct {
	file *os.File
	size int64
}

func NovoIndicePrimario(table string) (*IndicePrimario, error) {
	file, err := os.OpenFile(table+"-indice-primario.bin", os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &IndicePrimario{
		file: file,
		size: stat.Size(),
	}, nil
}

func (i *IndicePrimario) Inserir(id int64, endereco int64) error {
	offset := int64(0)
	aux := make([]byte, 20)
	var chave int64
	for {
		if -offset == i.size {
			// chegamos no inicio do arquivo, logo temos duas opções:
			// 1: é o primeiro registro sendo inserido
			// 2: é o menor registro presente no indice
			// ambos o registro é armazenado no inicio
			i.file.Seek(0, inicioArquivo)
			//escreve na proxima posiçao
			binary.PutVarint(aux[0:10], id)
			binary.PutVarint(aux[10:20], endereco)
			n, err := i.file.Write(aux)
			i.size += int64(n)
			return err
		}
		offset -= 20
		i.file.Seek(offset, finalArquivo)
		i.file.Read(aux)
		chave, _ = binary.Varint(aux[0:10])
		if chave == id {
			return duplicado
		}
		if chave < id {
			//achamos o registro que é menor que o id passado
			i.file.Seek(offset+20, finalArquivo)
			//escreve na proxima posiçao
			binary.PutVarint(aux[0:10], id)
			binary.PutVarint(aux[10:20], endereco)
			n, err := i.file.Write(aux)
			i.size += int64(n)
			return err
		}
	}
}

// Busca por um ID retornando o endereço associado
func (i *IndicePrimario) Busca(chave int64) (int64, bool) {
	qtd_registros := i.size / 20
	return i.buscaBinaria(chave, 0, qtd_registros)
}

func (i *IndicePrimario) buscaBinaria(chave, inicio, fim int64) (int64, bool) {
	if (fim - inicio) <= 0 {
		return 0, false
	}
	meio := ((inicio + fim) / 2) + 1
	i.file.Seek(meio*20, inicioArquivo)
	aux := make([]byte, 20)
	n, err := i.file.Read(aux)
	if err != nil || n != len(aux) {
		log.Fatalf("falhou ao ler os valores do arquivo de indice, leu %d bytes, err: %v", n, err)
	}
	id, _ := binary.Varint(aux[0:10])
	if id == chave {
		endereco, _ := binary.Varint(aux[10:20])
		return endereco, true
	}
	if chave < id {
		return i.buscaBinaria(chave, inicio, meio)
	}
	return i.buscaBinaria(chave, meio, fim)
}
