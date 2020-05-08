package sync

import (
	gosql "database/sql"
	"sync"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb-binlog/pkg/loader"
)

type TableInformations struct {
	tableInfos sync.Map
	db *gosql.DB
}

func NewTableInformations(user string, password string, host string, port int) (tis *TableInformations, err error) {
	tis = &TableInformations{}
	tis.db, err = initDB(user, password, host, port)
	if err != nil {
		return nil, err
	}
	return
}

func (tis *TableInformations) getTableInfo(schema, table string) (info *loader.TableInfo, err error){
	return loader.GetTableInfoExt(tis.db, schema, table)
}

func (tis *TableInformations) RefreshToInfos(schema, table string) (info *loader.TableInfo, err error) {
	info, err = tis.getTableInfo(schema, table)
	if err != nil {
		return nil, err
	}
	tis.tableInfos.Store(quoteSchema(schema, table), info)
	return info, nil
}

func (tis *TableInformations) GetFromInfos(schema, table string) (info *loader.TableInfo, err error) {
	v, ok := tis.tableInfos.Load(quoteSchema(schema, table))
	if ok {
		info = v.(*loader.TableInfo)
		return
	}

	return tis.RefreshToInfos(schema, table)
}

func (tis *TableInformations) EvitInfos(schema string, table string) {
	tis.tableInfos.Delete(quoteSchema(schema, table))
}

func (tis *TableInformations) NeedRefreshTableInfo(sql string) bool {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return true
	}

	switch stmt.(type) {
	case *ast.DropTableStmt:
		return false
	case *ast.DropDatabaseStmt:
		return false
	case *ast.TruncateTableStmt:
		return false
	case *ast.CreateDatabaseStmt:
		return false
	}

	return true
}

func initDB (user string, password string, host string, port int) (db *gosql.DB, err error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4,utf8&interpolateParams=true&readTimeout=1m&multiStatements=true", user, password, host, port)
	db, err = gosql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return db, nil
}

func quoteSchema(schema string, table string) string {
	return fmt.Sprintf("`%s`.`%s`", escapeName(schema), escapeName(table))
}

func escapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}