package db

import (
	"encoding/binary"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type Page struct {
	PageID  uint64
	isDirty bool
	numRows uint64
	data    []byte
}

func NewPage(pageID uint64) *Page {
	return &Page{
		PageID:  pageID,
		isDirty: false,
		numRows: 0,
		data:    make([]byte, PageSize),
	}
}

// CreatePage TODO: verify buffer
func CreatePage(pageID uint64, buffer []byte) *Page {
	data := make([]byte, PageSize)
	copy(data[:], buffer)
	page := &Page{
		PageID:  pageID,
		isDirty: false,
		numRows: uint64(len(buffer) / RowSize),
		data:    data,
	}
	return page
}

func (p *Page) Insert(tupleID uint64, r *Row) {
	offset := tupleID * RowSize
	copy(p.data[offset:offset+RowSize], r.serialize())
	p.numRows++
	p.isDirty = true
	return
}

func (p *Page) Row(tupleID uint64) *Row {
	offset := tupleID * RowSize
	return deserialize(p.data[offset : offset+RowSize])
}

func (p *Page) Rows() []*Row {
	rows := make([]*Row, p.numRows)
	var i uint64
	for i = 0; i < p.numRows; i++ {
		rows[i] = p.Row(i)
	}

	return rows
}

func (p *Page) Length() uint64 {
	return p.numRows * RowSize
}

func (p *Page) IsEmpty() bool {
	return p.numRows == 0
}

func (p *Page) LastTupleID() uint64 {
	return p.numRows - 1
}

func (p *Page) StartCursor() *PageCursor {
	return NewPageCursor(p)
}

func (p *Page) EndNextCursor() *PageCursor {
	return CreateEndNextPageCursor(p)
}

type PageCursor struct {
	page    *Page
	tupleID uint64
	isEnd   bool
}

func NewPageCursor(page *Page) *PageCursor {
	return &PageCursor{
		page:    page,
		tupleID: 0,
		isEnd:   page.IsEmpty(),
	}
}

func CreateEndNextPageCursor(page *Page) *PageCursor {
	return &PageCursor{
		page:    page,
		tupleID: page.numRows,
		isEnd:   true,
	}
}

func (pc *PageCursor) Advance() (err error) {
	if pc.page.IsEmpty() {
		return errors.New("cant advance page cursor on empty page")
	}

	pc.tupleID++
	if pc.tupleID == pc.page.numRows {
		pc.isEnd = true
	}
	return
}

func (pc *PageCursor) Row() *Row {
	log.WithField("tupleID", pc.tupleID).Info("retrieve tuple")
	return pc.page.Row(pc.tupleID)
}

func (pc *PageCursor) Insert(r *Row) {
	pc.page.Insert(pc.tupleID, r)
}

type NodeType uint16

const (
	InternalNodeType NodeType = iota
	LeafNodeType
)

const (
	CommonPageHeaderSize = 8
	LeafPageHeaderSize   = CommonPageHeaderSize + 4

	KeySize   = 4
	TupleSize = RowSize + KeySize

	LeafNodeMaxTuples = (PageSize - LeafPageHeaderSize) / TupleSize
)

type InternalPage struct {
}

type CommonPageHeader struct {
	Type         NodeType
	IsRoot       bool
	ParentPageID uint32
}

func (cp *CommonPageHeader) Serialize() []byte {
	buffer := make([]byte, CommonPageHeaderSize)
	binary.LittleEndian.PutUint16(buffer[:2], uint16(cp.Type))
	isRoot := uint16(0)
	if cp.IsRoot {
		isRoot = 1
	}
	binary.LittleEndian.PutUint16(buffer[2:4], isRoot)
	binary.LittleEndian.PutUint32(buffer[4:], cp.ParentPageID)
	return buffer
}

type LeafPageHeader struct {
	*CommonPageHeader
	NumTuples uint32
}

func (lph *LeafPageHeader) Serialize() []byte {
	buffer := make([]byte, CommonPageHeaderSize+4)
	copy(buffer[:CommonPageHeaderSize], lph.CommonPageHeader.Serialize())
	binary.LittleEndian.PutUint32(buffer[CommonPageHeaderSize:], lph.NumTuples)
	return buffer
}

type LeafPage struct {
	*LeafPageHeader
	PageID  uint32
	IsDirty bool
	data    []byte
}

func (lp *LeafPage) Serialize() []byte {
	buffer := make([]byte, PageSize)
	copy(buffer[:LeafPageHeaderSize], lp.LeafPageHeader.Serialize())
	copy(buffer[LeafPageHeaderSize:], lp.data)
	return buffer
}

func (lp *LeafPage) Tuple(tupleID uint32) *Row {
	offset := tupleID * TupleSize
	return deserialize(lp.data[offset+KeySize: offset + TupleSize])
}


func (lp *LeafPage) Insert(tupleID uint32, key uint32, r *Row) (err error) {
	if lp.NumTuples >= LeafNodeMaxTuples {
		return errors.New("Need to implement splitting a leaf node")
	}

	offset := tupleID * TupleSize
	binary.LittleEndian.PutUint32(lp.data[offset:offset+KeySize], key)
	copy(lp.data[offset+KeySize:offset+TupleSize], r.serialize())
	lp.NumTuples++
	lp.IsDirty = true
	return
}

func (lp *LeafPage) IsEmpty() bool {
	return lp.NumTuples == 0
}

type LeafPageCursor struct {
	leafPage *LeafPage
	tupleID  uint32
	isEnd    bool
}

func NewLeafPageStartCursor(leafPage *LeafPage) *LeafPageCursor {
	return &LeafPageCursor{
		leafPage: leafPage,
		tupleID:  0,
		isEnd:    leafPage.IsEmpty(),
	}
}

func NewLeafPageEndNextCursor(leafPage *LeafPage) *LeafPageCursor {
	return &LeafPageCursor{
		leafPage: leafPage,
		tupleID:  leafPage.NumTuples,
		isEnd:    true,
	}
}

func (lpc *LeafPageCursor) Next() (err error) {
	if lpc.leafPage.IsEmpty() {
		return errors.New("cant advance page cursor on empty page")
	}

	lpc.tupleID++
	if lpc.tupleID == lpc.leafPage.NumTuples {
		lpc.isEnd = true
	}
	return
}

func (lpc *LeafPageCursor) Tuple() *Row {
	log.WithField("tupleID", lpc.tupleID).Info("retrieve tuple")
	return lpc.leafPage.Tuple(lpc.tupleID)
}

func (lpc *LeafPageCursor) Insert(r *Row) error {
	return lpc.leafPage.Insert(lpc.tupleID, uint32(r.ID), r)
}
