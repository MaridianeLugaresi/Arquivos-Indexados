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
const tamanhoIndice = 20

var Duplicado error = errors.New("chave duplicada")
var NaoEncontrado error = errors.New("registro não encontrado")

type IndicePrimario struct {
	file *os.File
	size int64
}

type IndiceSecundarioNeN struct {
	file            *os.File
	size            int64
}
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
	deslocamento := int64(0)
	aux := make([]byte, tamanhoIndice)
	var chave int64
	for {
		if -deslocamento == i.size {
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
		deslocamento -= tamanhoIndice
		i.file.Seek(deslocamento, finalArquivo)
		i.file.Read(aux)
		chave, _ = binary.Varint(aux[0:10])
		if chave == id {
			return Duplicado
		}
		if chave < id {
			//achamos o registro que é menor que o id passado
			i.file.Seek(deslocamento+tamanhoIndice, finalArquivo)
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
func (i *IndicePrimario) Busca(chave int64) (int64, error) {
	qtdRegistros := i.size / tamanhoIndice
	return i.buscaBinaria(chave, 0, qtdRegistros)
}

func (i *IndicePrimario) buscaBinaria(chave, inicio, fim int64) (int64, error) {
	if (fim - inicio) <= 0 {
		return 0, NaoEncontrado
	}
	meio := ((inicio + fim) / 2) + 1
	i.file.Seek(meio*tamanhoIndice, inicioArquivo)
	aux := make([]byte, tamanhoIndice)
	n, err := i.file.Read(aux)
	if err != nil || n != len(aux) {
		log.Fatalf("falhou ao ler os valores do arquivo de indice, leu %d bytes, err: %v", n, err)
	}
	id, _ := binary.Varint(aux[0:10])
	if id == chave {
		endereco, _ := binary.Varint(aux[10:20])
		return endereco, nil
	}
	if chave < id {
		return i.buscaBinaria(chave, inicio, meio)
	}
	return i.buscaBinaria(chave, meio, fim)
}

func NovaIndiceSecundarioNeN(nome string) (*IndiceSecundarioNeN, error) {
	file, err := os.OpenFile(nome+"-relacao.bin", os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &IndiceSecundarioNeN{
		size: stat.Size(),
		file: file,
	}, nil
}

func (i *IndiceSecundarioNeN) Inserir(idPrincipal, idSecundario int64) error {
	deslocamento := int64(0)
	aux := make([]byte, tamanhoIndice)
	var chave int64
	for {
		if -deslocamento == i.size {
			// chegamos no inicio do arquivo, logo temos duas opções:
			// 1: é o primeiro registro sendo inserido
			// 2: é o menor registro presente no indice
			// ambos o registro é armazenado no inicio
			i.file.Seek(0, inicioArquivo)
			//escreve na proxima posiçao
			binary.PutVarint(aux[0:10], idPrincipal)
			binary.PutVarint(aux[10:20], idSecundario)
			n, err := i.file.Write(aux)
			i.size += int64(n)
			return err
		}
		deslocamento -= tamanhoIndice
		i.file.Seek(deslocamento, finalArquivo)
		i.file.Read(aux)
		chave, _ = binary.Varint(aux[0:10])
		if chave <= idPrincipal {
			//achamos o registro que é menor que o id passado
			i.file.Seek(deslocamento+tamanhoIndice, finalArquivo)
			//escreve na proxima posiçao
			binary.PutVarint(aux[0:10], idPrincipal)
			binary.PutVarint(aux[10:20], idSecundario)
			n, err := i.file.Write(aux)
			i.size += int64(n)
			return err
		}
	}
}

func (i *IndiceSecundarioNeN) BuscaPorIDPrimario(id int64) ([]int64, error) {
	qtdRegistros := i.size/tamanhoIndice
	return i.buscaBinariaIDPrimario(id, 0, qtdRegistros)
}


func (i *IndiceSecundarioNeN) buscaBinariaIDPrimario(chave, inicio, fim int64) ([]int64, error) {
	if (fim - inicio) <= 0 {
		return 0, NaoEncontrado
	}
	meio := ((inicio + fim) / 2) + 1
	i.file.Seek(meio*tamanhoIndice, inicioArquivo)
	aux := make([]byte, tamanhoIndice)
	n, err := i.file.Read(aux)
	if err != nil || n != len(aux) {
		log.Fatalf("falhou ao ler os valores do arquivo de indice, leu %d bytes, err: %v", n, err)
	}
	idPrimario, _ := binary.Varint(aux[0:10])
	if idPrimario == chave {
		idSecundario, _ := binary.Varint(aux[10:20])
		ids := []in64{idSecundario}
		deslocamento := meio*tamanhoIndice
		for {
			deslocamento -= tamanhoIndice
			i.file.Seek(deslocamento, inicioArquivo)
			n, err := i.file.Read(aux)
			if err != nil || n != len(aux) {
				log.Fatalf("falhou ao ler os valores do arquivo de indice, leu %d bytes, err: %v", n, err)
			}
			idPrimario, _ := binary.Varint(aux[0:10])
			if idPrimario != chave {
				break
			}
			idSecundario, _ := binary.Varint(aux[10:20])
			ids = append(ids, idSecundario)
		}
		deslocamento = meio*tamanhoIndice
		for {
			deslocamento += tamanhoIndice
			i.file.Seek(deslocamento, inicioArquivo)
			n, err := i.file.Read(aux)
			if err != nil || n != len(aux) {
				log.Fatalf("falhou ao ler os valores do arquivo de indice, leu %d bytes, err: %v", n, err)
			}
			idPrimario, _ := binary.Varint(aux[0:10])
			if idPrimario != chave {
				break
			}
			idSecundario, _ := binary.Varint(aux[10:20])
			ids = append(ids, idSecundario)
		}
		return ids, nil
	}
	if chave < id {
		return i.buscaBinaria(chave, inicio, meio)
	}
	return i.buscaBinaria(chave, meio, fim)
}
