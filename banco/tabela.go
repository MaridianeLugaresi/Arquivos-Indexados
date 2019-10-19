package banco

import "os"

type Tabela struct {
	nome            string
	tamanhoRegistro int64
	proximoEndereco int64

	file           *os.File
	indicePrimario *IndicePrimario
}

type Registro interface {
	Converte() []byte
	Desconverte(aux []byte) error
	ID() int64
}

func NovaTabela(nome string) (*Tabela, error) {
	file, err := os.OpenFile(nome+"-tabela.bin", os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}
	indice, err := NovoIndicePrimario(nome)
	if err != nil {
		return nil, err
	}
	return &Tabela{
		nome:           nome,
		file:           file,
		indicePrimario: indice,
	}, nil
}

func (t *Tabela) Inserir(registro Registro) error {
	t.file.Seek(0, finalArquivo)
	_, err := t.file.Write(registro.converte()) //escreve no arquivo db utilizando a função criada
	if err != nil {
		return err
	}
	return t.indicePrimario.Inserir(registro.ID(), t.proximoEndereco)
}
