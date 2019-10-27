package banco

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

//https://golang.org/pkg/os/#File.Seek
const inicioArquivo = 0
const finalArquivo = 2
const tamanhoIndice = 20

var Duplicado error = errors.New("chave duplicada")
var NaoEncontrado error = errors.New("registro não encontrado")

type IndicePrimario struct {
	file *os.File
	size int64
}

func NovoIndicePrimario(table string) (*IndicePrimario, error) {
	// abertura do arquivo e caso ele não exista será criado
	file, err := os.OpenFile(table+"-indice-primario.bin", os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}
	// Stat: retorna uma struct com informações sobre o arquivo ou um erro
	// utilizada para pegar o tamanho do arquivo em bytes
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
	deslocamento := int64(0)
	var chave int64
	for {
		if -deslocamento == i.size {
			// chegamos no inicio do arquivo, logo temos duas opções:
			// 1: é o primeiro registro sendo inserido
			// 2: é o menor registro presente no indice
			// ambos o registro é armazenado no inicio
			i.file.Seek(0, inicioArquivo)
			// ReadAll le todo o arquivo a partir da posição atual ao final, o cursor vai estar apontando para o final do arquivo
			restante, err := ioutil.ReadAll(i.file)
			if err != nil {
				return fmt.Errorf("Falha ao ler o restante do arquivo, err: %v", err)
			}
			i.file.Seek(0, inicioArquivo)
			// aloca tamanhoIndice de bytes nulos
			aux := make([]byte, tamanhoIndice)
			// PutVarint grava o id e o endereço no Slice alocado anteriormente
			binary.PutVarint(aux[0:10], id)
			binary.PutVarint(aux[10:20], endereco)
			n, err := i.file.Write(aux)
			if err != nil {
				return fmt.Errorf("Falha ao escrever no arquivo de indice, err: %v", err)
			}
			// incrementa o tamanho do arquivo
			i.size += int64(n)
			_, err = i.file.Write(restante)
			if err != nil {
				return fmt.Errorf("Falha ao escrever o restante no arquivo de indice, err: %v", err)
			}
			return nil
		}
		// Caso não seje o primeiro nem o menor executa o restante dos comandos
		deslocamento -= tamanhoIndice
		i.file.Seek(deslocamento, finalArquivo)
		// aloca tamanhoIndice de bytes nulos
		aux := make([]byte, tamanhoIndice)
		i.file.Read(aux)
		// Varint retorna o valor e o número de bytes lidos
		chave, _ = binary.Varint(aux[0:10])
		if chave == id {
			return Duplicado
		}
		if chave < id {
			//achamos o registro que é menor que o id passado
			i.file.Seek(deslocamento+tamanhoIndice, finalArquivo)
			//ReadAll le todo o arquivo a partir da posição atual ao final, o cursor vai estar apontando para o final do arquivo
			restante, err := ioutil.ReadAll(i.file) 
			if err != nil {
				return fmt.Errorf("Falha ao ler o restante do arquivo, err: %v", err)
			}
			i.file.Seek(deslocamento+tamanhoIndice, finalArquivo) // voltamos o cursor para a posição antiga
			//escreve na proxima posiçao
			aux := make([]byte, tamanhoIndice)
			binary.PutVarint(aux[0:10], id)
			binary.PutVarint(aux[10:20], endereco)
			n, err := i.file.Write(aux)
			if err != nil {
				return fmt.Errorf("Falha ao escrever no arquivo de indice, err: %v", err)
			}
			//incrementa o tamanho do arquivo
			i.size += int64(n)
			_, err = i.file.Write(restante)
			if err != nil {
				return fmt.Errorf("Falha ao escrever o restante no arquivo de indice, err: %v", err)
			}
			return nil
		}
	}
}

// Busca por um ID retornando o endereço associado
func (i *IndicePrimario) Busca(chave int64) (int64, error) {
	qtdRegistros := i.size / tamanhoIndice
	if qtdRegistros == 0 {
		return 0, NaoEncontrado
	}
	return i.buscaBinaria(chave, 0, qtdRegistros-1)
}

func (i *IndicePrimario) buscaBinaria(chave, inicio, fim int64) (int64, error) {
	meio := ((inicio + fim) / 2)
	i.file.Seek(meio*tamanhoIndice, inicioArquivo)
	aux := make([]byte, tamanhoIndice)
	n, err := i.file.Read(aux)
	if err != nil && n != len(aux) {
		log.Fatalf("Falhou ao ler os valores do arquivo de indice, leu %d bytes, err: %v", n, err)
	}
	id, _ := binary.Varint(aux[0:10])
	if id == chave {
		endereco, _ := binary.Varint(aux[10:20])
		return endereco, nil
	}
	if inicio >= fim {
		return 0, NaoEncontrado
	}
	if chave < id {
		return i.buscaBinaria(chave, inicio, meio-1)
	}
	return i.buscaBinaria(chave, meio+1, fim)
}

func (i *IndicePrimario) Close() error {
	return i.file.Close()
}
