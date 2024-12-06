package mysqlstorage

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/akaxb/gocap/storage"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"strings"
	"sync"
)

var _ storage.IStorageInitializer = &MysqlInitializer{}

func GenerateConnectionString(username, pwd, host, port, dbname string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=True&loc=Local&multiStatements=true", username, pwd, host, port, dbname)
}

type MysqlInitializer struct {
	TableNamePrefix string
	logger          *log.Logger
	conn            string
	once            sync.Once
}

func NewMysqlInitializer(conn string, tableNamePrefix string, logger *log.Logger) *MysqlInitializer {
	return &MysqlInitializer{
		TableNamePrefix: tableNamePrefix,
		logger:          logger,
		conn:            conn,
	}
}

func (i *MysqlInitializer) Initialize(ctx context.Context) {
	i.once.Do(func() {
		db, err := sql.Open("mysql", i.conn)
		if err != nil {
			i.logger.Fatalf("Open db error:%v", err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				i.logger.Printf("Error: Failed to close db err:%s", err)
			}
		}()
		if _, err = db.Exec(i.createDbTablesScript()); err != nil {
			i.logger.Fatalf("Fatal: Create table error:%v", err)
		}
	})
}

func (i *MysqlInitializer) GetPublishedTableName() string {
	return strings.Join([]string{i.TableNamePrefix, "Published"}, ".")
}

func (i *MysqlInitializer) GetReceivedTableName() string {
	return strings.Join([]string{i.TableNamePrefix, "Received"}, ".")
}

func (i *MysqlInitializer) createDbTablesScript() string {
	batchSql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` ("+
		"`Id` bigint NOT NULL,`"+
		"Version` varchar(20) DEFAULT NULL,"+
		"`Name` varchar(400) NOT NULL,"+
		"`Group` varchar(200) DEFAULT NULL,"+
		"`Content` longtext,"+
		"`Retries` int(11) DEFAULT NULL,"+
		"`Added` datetime NOT NULL,"+
		"`ExpiresAt` datetime DEFAULT NULL,"+
		"`StatusName` varchar(50) NOT NULL,"+
		"PRIMARY KEY (`Id`),"+
		"INDEX `IX_ExpiresAt`(`ExpiresAt`))"+
		"ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;", i.GetReceivedTableName())
	batchSql = batchSql + fmt.Sprintf(" CREATE TABLE IF NOT EXISTS `%s` ("+
		"`Id` bigint NOT NULL, "+
		"`Version` varchar(20) DEFAULT NULL,"+
		"`Name` varchar(200) NOT NULL,"+
		"`Content` longtext,"+
		"`Retries` int(11) DEFAULT NULL,"+
		"`Added` datetime NOT NULL,"+
		"`ExpiresAt` datetime DEFAULT NULL,"+
		"`StatusName` varchar(40) NOT NULL,"+
		"PRIMARY KEY (`Id`),"+
		"INDEX `IX_ExpiresAt`(`ExpiresAt`)"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
		i.GetPublishedTableName())
	return batchSql
}
