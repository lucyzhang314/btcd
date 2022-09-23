// Copyright (c) 2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package mining

import "github.com/sirupsen/logrus"

// log is a logger that is initialized with no output filters.  This
// means the package will not perform any logging by default until the caller
// requests it.
var log *logrus.Entry

// UseLogger uses a specified Logger to output package logging info.
func UseLogger(logger *logrus.Entry) {
	log = logger
}
