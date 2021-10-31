package db

import (
	"os"

	"github.com/pkg/errors"
)

type Pager struct {
	FileName string
	FileLength uint64
	File *os.File
	Pages map[uint64]*Page
}


func NewPager(fileName string) *Pager {
	return &Pager{
		FileName: fileName,
		Pages: make(map[uint64]*Page),
	}
}

func (p *Pager) Open() (err error) {
	if p.File, err = os.OpenFile(p.FileName, os.O_RDWR|os.O_CREATE, 0755); err != nil {
		return err
	}

	var fileInfo os.FileInfo
	if fileInfo, err = p.File.Stat(); err != nil {
		return
	}

	p.FileLength = uint64(fileInfo.Size())
	if p.FileLength % PageSize != 0 {
		return errors.New("Db file is not whole number of pages. Corrupt file.")
	}
	return
}

// NumPages returns the number of the pages in the db file.
func (p *Pager) NumPages() uint64 {
	numPages := p.FileLength / PageSize
	if p.FileLength % PageSize > 0 {
		numPages++
	}
	return numPages
}

func (p *Pager) LastPageLength() uint64 {
	lastPageLength := uint64(PageSize)
	if length := p.FileLength % PageSize; length > 0 {
		lastPageLength = length
	}

	return lastPageLength
}

func (p *Pager) GetPage(pageID uint64) (*Page, error) {
	if pageID >= TableMaxPages {
		return nil, errors.Errorf("Tried to fetch page number out of bounds. %d", pageID)
	}

	if _, ok := p.Pages[pageID]; !ok {
		var page *Page
		var err error
		if pageID >= p.NumPages() {
			page = NewPage(pageID)
		} else {
			if page, err = p.ReadPageFromDBFile(pageID); err != nil {
				return nil, err
			}
		}
		p.Pages[pageID] = page
	}

	return p.Pages[pageID], nil
}

func (p *Pager) ReadPageFromDBFile(pageID uint64) (*Page, error) {
	var pageLength uint64
	if pageID == p.NumPages() - 1 {
		pageLength = p.LastPageLength()
	} else {
		pageLength = PageSize
	}

	buffer, err := ReadFile(p.File, pageLength, pageID * PageSize)
	if err != nil {
		return nil, err
	}
	return CreatePage(pageID, buffer), nil
}

func (p *Pager) Close() (err error) {
	for pageID, page := range p.Pages {
		if page.isDirty {
			if err = WriteFile(p.File, page.data, page.Length(), pageID * PageSize); err != nil {
				return err
			}
		}
	}
	if err = p.File.Close(); err != nil {
		return errors.Wrapf(err, "close db file error")
	}
	return
}