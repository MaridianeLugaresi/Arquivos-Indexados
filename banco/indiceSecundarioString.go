package banco

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

type IndiceSecundarioString struct {
	file            *os.File
	size            int64
	tamanhoValor    int64
	tamanhoRegistro int64
	unico           bool
}

func NovoIndiceSecundarioString(nome string, tamanho int64, unico bool) (*IndiceSecundarioString, error) {
	file, err := os.OpenFile(nome+"-indice.bin", os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &IndiceSecundarioString{
		size:            stat.Size(),
		file:            file,
		tamanhoValor:    tamanho,
		tamanhoRegistro: tamanho + 10,
		unico:           unico,
	}, nil
}

func (i *IndiceSecundarioString) Inserir(valor string, endereco int64) error {
	if i.size == 0 {
		// primeiro registro, logo é só inserir
		i.file.Seek(0, inicioArquivo)
		//escreve na proxima posiçao
		aux := make([]byte, i.tamanhoRegistro)
		copy(aux[0:i.tamanhoValor], valor)
		binary.PutVarint(aux[i.tamanhoValor:i.tamanhoValor+10], endereco)
		n, err := i.file.Write(aux)
		if err != nil {
			return fmt.Errorf("falha ao escrever no arquivo de indice, err: %v", err)
		}
		i.size += int64(n)
		i.file.Sync()
		return nil
	}
	deslocamento := int64(0)
	for {
		deslocamento -= i.tamanhoRegistro
		i.file.Seek(deslocamento, finalArquivo)
		aux := make([]byte, i.tamanhoRegistro)
		i.file.Read(aux)
		chave := strings.Trim(string(aux[0:i.tamanhoValor]), "\x00")
		if chave == valor && i.unico {
			return Duplicado
		}
		if chave <= valor {
			//achamos o registro que é menor que o id passado ou igual se o indice não é unico.
			// logo escrevemos no proximo bloco
			i.file.Seek(deslocamento+i.tamanhoRegistro, finalArquivo)
			restante, err := ioutil.ReadAll(i.file) // ReadAll le todo o arquivo a partir da posição atual e ao final, o cursor vai estar apontando para o final do arquivo
			if err != nil {
				return fmt.Errorf("falha ao ler o restante do arquivo, err: %v", err)
			}
			i.file.Seek(deslocamento+i.tamanhoRegistro, finalArquivo) // voltamos o cursor para a posição antiga
			//escreve na proxima posiçao
			aux := make([]byte, i.tamanhoRegistro)
			copy(aux[0:i.tamanhoValor], valor)
			binary.PutVarint(aux[i.tamanhoValor:i.tamanhoValor+10], endereco)
			n, err := i.file.Write(aux)
			if err != nil {
				return fmt.Errorf("falha ao escrever no arquivo de indice, err: %v", err)
			}
			i.size += int64(n)
			_, err = i.file.Write(restante)
			if err != nil {
				return fmt.Errorf("falha ao escrever o restante no arquivo de indice, err: %v", err)
			}
			//como estamos lendo ao mesmo tempo que inserimos precisamos fazer o flush para o disco
			i.file.Sync()
			return nil
		}
		if -deslocamento == i.size {
			// chegamos no inicio do arquivo, logo:  é o menor registro presente no indice
			i.file.Seek(0, inicioArquivo)
			restante, err := ioutil.ReadAll(i.file) // ReadAll le todo o arquivo a partir da posição atual e ao final, o cursor vai estar apontando para o final do arquivo
			if err != nil {
				return fmt.Errorf("falha ao ler o restante do arquivo, err: %v", err)
			}
			i.file.Seek(0, inicioArquivo) // voltamos o cursor para a posição antiga
			//escreve na proxima posiçao
			aux := make([]byte, i.tamanhoRegistro)
			copy(aux[0:i.tamanhoValor], valor)
			binary.PutVarint(aux[i.tamanhoValor:i.tamanhoValor+10], endereco)
			n, err := i.file.Write(aux)
			if err != nil {
				return fmt.Errorf("falha ao escrever no arquivo de indice, err: %v", err)
			}
			i.size += int64(n)
			_, err = i.file.Write(restante)
			if err != nil {
				return fmt.Errorf("falha ao escrever o restante no arquivo de indice, err: %v", err)
			}
			i.file.Sync()
			return nil
		}
	}
}

func (i *IndiceSecundarioString) BuscaPorValor(valor string) ([]int64, error) {
	qtdRegistros := i.size / i.tamanhoRegistro
	if qtdRegistros == 0 {
		return []int64{}, NaoEncontrado
	}
	return i.buscaBinaria(valor, 0, qtdRegistros-1)
}

func (i *IndiceSecundarioString) buscaBinaria(chave string, inicio, fim int64) ([]int64, error) {
	meio := ((inicio + fim) / 2)
	_, err := i.file.Seek(meio*i.tamanhoRegistro, inicioArquivo)
	if err != nil {
		log.Fatalf("Falha ao mover o cursor na posição desejada %d, err: %v", meio*i.tamanhoRegistro, err)
	}
	aux := make([]byte, i.tamanhoRegistro)
	n, err := i.file.Read(aux)
	if err != nil && n != len(aux) {
		log.Fatalf("1-falhou ao ler os valores do arquivo de indice, leu %d bytes, err: %v", n, err)
	}
	valor := strings.Trim(string(aux[0:i.tamanhoValor]), "\x00")
	if valor == chave {
		end, _ := binary.Varint(aux[i.tamanhoValor : i.tamanhoValor+10])
		enderecos := []int64{end}
		if i.unico {
			return enderecos, nil
		}
		deslocamento := meio * i.tamanhoRegistro
		for {
			deslocamento -= i.tamanhoRegistro
			if deslocamento < 0 {
				// inicio do arquivo
				break
			}
			i.file.Seek(deslocamento, inicioArquivo)
			n, err := i.file.Read(aux)
			if err != nil && n != len(aux) {
				log.Fatalf("2-falhou ao ler os valores do arquivo de indice, leu %d bytes, err: %v", n, err)
			}
			valor := strings.Trim(string(aux[0:i.tamanhoValor]), "\x00")
			if valor != chave {
				break
			}
			end, _ := binary.Varint(aux[i.tamanhoValor : i.tamanhoValor+10])
			enderecos = append(enderecos, end)
		}
		deslocamento = meio * i.tamanhoRegistro
		for {
			deslocamento += i.tamanhoRegistro
			if deslocamento > i.size {
				// fim do arquivo
				break
			}
			i.file.Seek(deslocamento, inicioArquivo)
			n, err := i.file.Read(aux)
			if err != nil && n != len(aux) {
				log.Fatalf("3-falhou ao ler os valores do arquivo de indice, leu %d bytes, err: %v", n, err)
			}
			valor := strings.Trim(string(aux[0:i.tamanhoValor]), "\x00")
			if valor != chave {
				break
			}
			end, _ := binary.Varint(aux[i.tamanhoValor : i.tamanhoValor+10])
			enderecos = append(enderecos, end)
		}
		return enderecos, nil
	}
	if inicio >= fim {
		return []int64{}, NaoEncontrado
	}
	if chave < valor {
		return i.buscaBinaria(chave, inicio, meio-1)
	}
	return i.buscaBinaria(chave, meio+1, fim)
}

func (i *IndiceSecundarioString) Close() error {
	return i.file.Close()
}
