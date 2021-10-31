package db

import (
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"os"
)

func ReadFile(file *os.File, total, offset uint64) ([]byte, error) {
	log.WithFields(log.Fields{
		"file_name": file.Name(),
		"total": total,
		"offset": offset,
	}).Info("start to read file")

	var cur uint64
	var n int
	buffer := make([]byte, total)
	var err error
	for i := 0; i < 3; i++ {
		if cur >= total {
			break
		}

		if _, err = file.Seek(int64(offset+cur), 0); err != nil {
			return nil, errors.WithStack(err)
		}

		if n, err = file.Read(buffer[cur:]); err != nil {
			return nil, errors.WithStack(err)
		}

		cur += uint64(n)
	}

	if cur < total {
		return nil, errors.WithStack(errors.Errorf("can not read enough bytes cur = %d", cur))
	}

	log.WithField("len", total).Info("successfully read file")
	return buffer, nil
}

func WriteFile(file *os.File, buffer []byte, total, offset uint64) error {
	log.WithFields(log.Fields{
		"file_name": file.Name(),
		"buffer_len": len(buffer),
		"total": total,
		"offset": offset,
	}).Info("start to write file")

	var cur uint64
	var n int
	var err error
	for i := 0; i < 3; i++ {
		if cur >= total {
			break
		}

		if _, err = file.Seek(int64(offset + cur), 0); err != nil {
			return errors.WithStack(err)
		}

		if n, err = file.Write(buffer[cur:total]); err != nil {
			return errors.WithStack(err)
		}

		cur += uint64(n)
	}

	if cur < total {
		return errors.WithStack(errors.Errorf("can not write enough bytes cur = %d", cur))
	}

	log.WithField("len", total).Info("successfully write to file")
	return nil
}
