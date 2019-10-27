package banco

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

type IndiceSecundarioNeN struct {
	file *os.File
	size int64
}

func NovoIndiceSecundarioNeN(nome string) (*IndiceSecundarioNeN, error) {
	file, err := os.OpenFile(nome+"-relacao.bin", os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}
	// stat retorna uma struct com informações sobre o arquivo
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
	var chave int64
	for {
		if -deslocamento == i.size {
			// chegamos no inicio do arquivo, logo temos duas opções:
			// 1: é o primeiro registro sendo inserido
			// 2: é o menor registro presente no indice
			// ambos o registro é armazenado no inicio
			i.file.Seek(0, inicioArquivo)
			restante, err := ioutil.ReadAll(i.file) // ReadAll le todo o arquivo a partir da posição atual e ao final, o cursor vai estar apontando para o final do arquivo
			if err != nil {
				return fmt.Errorf("falha ao ler o restante do arquivo, err: %v", err)
			}
			i.file.Seek(0, inicioArquivo) // voltamos o cursor para a posição antiga
			//escreve na proxima posiçao
			aux := make([]byte, tamanhoIndice)
			binary.PutVarint(aux[0:10], idPrincipal)
			binary.PutVarint(aux[10:20], idSecundario)
			n, err := i.file.Write(aux)
			if err != nil {
				return fmt.Errorf("falha ao escrever no arquivo de indice, err: %v", err)
			}
			i.size += int64(n)
			_, err = i.file.Write(restante)
			if err != nil {
				return fmt.Errorf("falha ao escrever o restante no arquivo de indice, err: %v", err)
			}
			return nil
		}
		deslocamento -= tamanhoIndice
		aux := make([]byte, tamanhoIndice)
		i.file.Seek(deslocamento, finalArquivo)
		i.file.Read(aux)
		chave, _ = binary.Varint(aux[0:10])
		if chave <= idPrincipal {
			//achamos o registro que é menor que o id passado
			i.file.Seek(deslocamento+tamanhoIndice, finalArquivo)
			restante, err := ioutil.ReadAll(i.file) // ReadAll le todo o arquivo a partir da posição atual e ao final, o cursor vai estar apontando para o final do arquivo
			if err != nil {
				return fmt.Errorf("falha ao ler o restante do arquivo, err: %v", err)
			}
			i.file.Seek(deslocamento+tamanhoIndice, finalArquivo) // voltamos o cursor para a posição antiga
			//escreve na proxima posiçao
			aux := make([]byte, tamanhoIndice)
			binary.PutVarint(aux[0:10], idPrincipal)
			binary.PutVarint(aux[10:20], idSecundario)
			n, err := i.file.Write(aux)
			if err != nil {
				return fmt.Errorf("falha ao escrever no arquivo de indice, err: %v", err)
			}
			i.size += int64(n)
			_, err = i.file.Write(restante)
			if err != nil {
				return fmt.Errorf("falha ao escrever o restante no arquivo de indice, err: %v", err)
			}
			return nil
		}
	}
}

func (i *IndiceSecundarioNeN) BuscaPorIDPrimario(id int64) ([]int64, error) {
	qtdRegistros := i.size / tamanhoIndice
	if qtdRegistros == 0 {
		return []int64{}, NaoEncontrado
	}
	return i.buscaBinariaIDPrimario(id, 0, qtdRegistros-1)
}

func (i *IndiceSecundarioNeN) buscaBinariaIDPrimario(chave, inicio, fim int64) ([]int64, error) {
	meio := ((inicio + fim) / 2)
	i.file.Seek(meio*tamanhoIndice, inicioArquivo)
	aux := make([]byte, tamanhoIndice)
	n, err := i.file.Read(aux)
	if err != nil && n != len(aux) {
		log.Fatalf("falhou ao ler os valores do arquivo de indice, leu %d bytes, err: %v", n, err)
	}
	idPrimario, _ := binary.Varint(aux[0:10])
	if idPrimario == chave {
		idSecundario, _ := binary.Varint(aux[10:20])
		ids := []int64{idSecundario}
		deslocamento := meio * tamanhoIndice
		for {
			deslocamento -= tamanhoIndice
			if deslocamento < 0 {
				// inicio do arquivo
				break
			}
			i.file.Seek(deslocamento, inicioArquivo)
			n, err := i.file.Read(aux)
			if err != nil && n != len(aux) {
				log.Fatalf("falhou ao ler os valores do arquivo de indice, leu %d bytes, err: %v", n, err)
			}
			idPrimario, _ := binary.Varint(aux[0:10])
			if idPrimario != chave {
				break
			}
			idSecundario, _ := binary.Varint(aux[10:20])
			ids = append(ids, idSecundario)
		}
		deslocamento = meio * tamanhoIndice
		for {
			deslocamento += tamanhoIndice
			if deslocamento > i.size {
				// fim do arquivo
				break
			}
			i.file.Seek(deslocamento, inicioArquivo)
			n, err := i.file.Read(aux)
			if err != nil && n != len(aux) {
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
	if inicio >= fim {
		return []int64{}, NaoEncontrado
	}
	if chave < idPrimario {
		return i.buscaBinariaIDPrimario(chave, inicio, meio-1)
	}
	return i.buscaBinariaIDPrimario(chave, meio+1, fim)
}

func (i *IndiceSecundarioNeN) Close() error {
	return i.file.Close()
}
