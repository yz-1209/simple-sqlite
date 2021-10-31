package db

import (
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	TableMaxPages = 100
	PageSize = 4096
	RowsPerPage = PageSize / RowSize
	TableMaxRows = RowsPerPage * TableMaxPages
)

type Table struct {
	numRows uint64
	RootPageID uint32
	pager *Pager
}

func NewTable(db string) *Table {
	return &Table{
		pager: NewPager(db),
		numRows: 0,
	}
}

func (t *Table) Open() (err error) {
	if err = t.pager.Open(); err != nil {
		return
	}

	t.numRows = t.pager.FileLength / RowSize
	return
}

func (t *Table) Close() error {
	return t.pager.Close()
}

func (t *Table) IsFull() bool {
	return t.numRows >= TableMaxRows
}

func (t *Table) IsEmpty() bool {
	return t.numRows <= 0
}

func (t *Table) LastRowID() uint64 {
	return t.numRows - 1
}

func (t *Table) GetPage(rowID uint64) (*Page, error) {
	pageID := GenPageID(rowID)
	return t.pager.GetPage(pageID)
}

func (t *Table) AppendRow(r *Row) error {
	cursor, err := t.EndNextCursor()
	if err != nil {
		return err
	}

	cursor.Insert(r)
	t.numRows++
	return nil
}

func (t *Table) InsertRow(rowID uint64, r *Row) error {
	pageID := GenPageID(rowID)
	page, err := t.pager.GetPage(pageID)
	if err != nil {
		return err
	}

	tupleID := GenTupleID(rowID)
	page.Insert(tupleID, r)
	t.numRows++
	return nil
}

func GenPageID(rowNum uint64) uint64 {
	return rowNum / RowsPerPage
}

func GenTupleID(rowNum uint64) uint64 {
	return rowNum % RowsPerPage
}

func (t *Table) SelectRows() (rows []*Row, err error) {
	var cursor *TableCursor
	if cursor, err = t.StartCursor(); err != nil {
		return
	}

	for !cursor.isEnd {
		rows = append(rows, cursor.Row())
		if err = cursor.Advance(); err != nil {
			return
		}
	}
	return
}

func (t *Table) EndNextCursor() (*TableCursor, error) {
	return CreateTableEndNextCursor(t)
}

func (t *Table) StartCursor() (*TableCursor, error) {
	return CreateTableStartCursor(t)
}


type TableCursor struct {
	table *Table
	pageCursor *PageCursor
	rowID uint64
	// isEnd when isEnd = true, cursor points to an empty tuple, which is next of the actual end tuple
	isEnd bool
}

func CreateTableStartCursor(t *Table) (*TableCursor, error) {
	rowID := uint64(0)
	page, err := t.GetPage(rowID)
	if err != nil {
		return nil, err
	}

	return &TableCursor{
		table:      t,
		pageCursor: page.StartCursor(),
		rowID:      0,
		isEnd:      t.IsEmpty(),
	}, nil
}

func CreateTableEndNextCursor(t *Table) (*TableCursor, error) {
	rowID := t.numRows
	page, err := t.GetPage(rowID)
	if err != nil {
		return nil, err
	}
	return &TableCursor{
		table: t,
		pageCursor: page.EndNextCursor(),
		rowID: rowID,
		isEnd: true,
	}, nil
}

func (tc *TableCursor) Advance() (err error) {
	if tc.table.IsEmpty() {
		return errors.New("cant advance cursor on an empty table")
	}

	log.WithFields(log.Fields{"tc.rowID": tc.rowID, "lastRowID": tc.table.LastRowID()}).Info("table cursor advance")
	tc.rowID++
	if tc.rowID == tc.table.numRows {
		tc.isEnd = true
		return
	}

	if tc.pageCursor.isEnd {
		var nextPage *Page
		if nextPage, err = tc.table.GetPage(tc.rowID); err != nil {
			return
		}

		tc.pageCursor = nextPage.StartCursor()
		return
	}

	return tc.pageCursor.Advance()
}

func (tc *TableCursor) Insert(r *Row) {
	tc.pageCursor.Insert(r)
}

func (tc *TableCursor) Row() *Row {
	return tc.pageCursor.Row()
}