package main

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"runtime/debug"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func fatalOnError(err error) {
	if err != nil {
		tm := time.Now()
		fmt.Printf("Error(time=%+v):\nError: '%s'\nStacktrace:\n%s\n", tm, err.Error(), string(debug.Stack()))
		fmt.Fprintf(os.Stderr, "Error(time=%+v):\nError: '%s'\nStacktrace:\n", tm, err.Error())
		panic("stacktrace")
	}
}

func fatalf(f string, a ...interface{}) {
	fatalOnError(fmt.Errorf(f, a...))
}

func queryOut(query string, args ...interface{}) {
	fmt.Printf("%s\n", query)
	if len(args) > 0 {
		s := ""
		for vi, vv := range args {
			switch v := vv.(type) {
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, complex64, complex128, string, bool, time.Time:
				s += fmt.Sprintf("%d:%+v ", vi+1, v)
			case *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64, *float32, *float64, *complex64, *complex128, *string, *bool, *time.Time:
				s += fmt.Sprintf("%d:%+v ", vi+1, v)
			case nil:
				s += fmt.Sprintf("%d:(null) ", vi+1)
			default:
				s += fmt.Sprintf("%d:%+v ", vi+1, reflect.ValueOf(vv).Elem())
			}
		}
		fmt.Printf("[%s]\n", s)
	}
}

func query(db *sql.DB, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		queryOut(query, args...)
	}
	return rows, err
}

func exec(db *sql.Tx, query string, args ...interface{}) (sql.Result, error) {
	res, err := db.Exec(query, args...)
	if err != nil {
		queryOut(query, args...)
	}
	return res, err
}

// setOrgDomain: API params: 'organization_name' 'domain' [overwrite] [top]
// if overwrite is set, all profiles found are force-updated/affiliated to 'organization_name'
// if overwite is not set, API will not change any profiles which already have any affiliation(s)
// if you specify "top" as 4th argument it will set 'is_top_domain' cvalue to true, else it will set false
func setOrgDomain(db *sql.DB, args []string) (info string, err error) {
	if len(args) < 2 {
		fatalf("setOrgDomain: requires 2 args: organization name & domain")
	}
	org := args[0]
	dom := args[1]
	overwrite := false
	isTopDomain := false
	if len(args) >= 3 {
		overwrite = args[2] == "overwrite"
	}
	if len(args) >= 4 {
		isTopDomain = args[3] == "top"
	}
	rows, err := query(db, "select id from organizations where name = ?", org)
	fatalOnError(err)
	var orgID int
	fetched := false
	for rows.Next() {
		fatalOnError(rows.Scan(&orgID))
		fetched = true
	}
	fatalOnError(rows.Err())
	fatalOnError(rows.Close())
	if !fetched {
		info = fmt.Sprintf("cannot find organization '%s'", org)
		err = fmt.Errorf("%s", info)
		return
	}
	rows, err = query(db, "select 1 from domains_organizations where organization_id = ? and domain = ?", orgID, dom)
	fatalOnError(err)
	dummy := 0
	for rows.Next() {
		fatalOnError(rows.Scan(&dummy))
	}
	fatalOnError(rows.Err())
	fatalOnError(rows.Close())
	if dummy == 1 {
		info = fmt.Sprintf("domain '%s' is already assigned to organization '%s'", dom, org)
		return
	}
	con, err := db.Begin()
	fatalOnError(err)
	_, err = exec(
		con,
		"insert into domains_organizations(organization_id, domain, is_top_domain) select ?, ?, ?",
		orgID,
		dom,
		isTopDomain,
	)
	fatalOnError(err)
	if overwrite {
		res, err := exec(
			con,
			"delete from enrollments where uuid in (select distinct sub.uuid from ("+
				"select distinct uuid from profiles where email like ? "+
				"union select distinct uuid from identities where email like ?) sub)",
			"%"+dom,
			"%"+dom,
		)
		fatalOnError(err)
		affected, err := res.RowsAffected()
		fatalOnError(err)
		if affected > 0 {
			info = fmt.Sprintf("deleted: %d", affected)
		}
		res, err = exec(
			con,
			"insert into enrollments(start, end, uuid, organization_id) "+
				"select distinct sub.start, sub.end, sub.uuid, sub.org_id from ("+
				"select '1900-01-01 00:00:00' as start, '2100-01-01 00:00:00' as end, uuid, ? as org_id from profiles where email like ? "+
				"union select '1900-01-01 00:00:00', '2100-01-01 00:00:00', uuid, ? from identities where email like ?) sub",
			orgID,
			"%"+dom,
			orgID,
			"%"+dom,
		)
		fatalOnError(err)
		affected, err = res.RowsAffected()
		fatalOnError(err)
		if affected > 0 {
			if info == "" {
				info = fmt.Sprintf("inserted: %d", affected)
			} else {
				info += fmt.Sprintf("\ninserted: %d", affected)
			}
		}
	} else {
		res, err := exec(
			con,
			"insert into enrollments(start, end, uuid, organization_id) "+
				"select distinct sub.start, sub.end, sub.uuid, sub.org_id from ("+
				"select '1900-01-01 00:00:00' as start, '2100-01-01 00:00:00' as end, uuid, ? as org_id from profiles where email like ? "+
				"union select '1900-01-01 00:00:00', '2100-01-01 00:00:00', uuid, ? from identities where email like ?) sub "+
				"where sub.uuid not in (select distinct uuid from enrollments)",
			orgID,
			"%"+dom,
			orgID,
			"%"+dom,
		)
		fatalOnError(err)
		affected, err := res.RowsAffected()
		fatalOnError(err)
		if affected > 0 {
			info = fmt.Sprintf("inserted: %d", affected)
		}
	}
	fatalOnError(con.Commit())
	return
}

// getConnectString - get MariaDB SH (Sorting Hat) database DSN
// Either provide full DSN via SH_DSN='shuser:shpassword@tcp(shhost:shport)/shdb?charset=utf8&parseTime=true'
// Or use some SH_ variables, only SH_PASS is required
// Defaults are: "shuser:required_pwd@tcp(localhost:3306)/shdb?charset=utf8"
// SH_DSN has higher priority; if set no SH_ varaibles are used
func getConnectString(prefix string) string {
	//dsn := "shuser:"+os.Getenv("PASS")+"@/shdb?charset=utf8")
	dsn := os.Getenv(prefix + "DSN")
	if dsn == "" {
		pass := os.Getenv(prefix + "PASS")
		user := os.Getenv(prefix + "USR")
		if user == "" {
			user = os.Getenv(prefix + "USER")
		}
		proto := os.Getenv(prefix + "PROTO")
		if proto == "" {
			proto = "tcp"
		}
		host := os.Getenv(prefix + "HOST")
		if host == "" {
			host = "localhost"
		}
		port := os.Getenv(prefix + "PORT")
		if port == "" {
			port = "3306"
		}
		db := os.Getenv(prefix + "DB")
		if db == "" {
			fatalf("please specify database via %sDB=...", prefix)
		}
		params := os.Getenv(prefix + "PARAMS")
		if params == "" {
			params = "?charset=utf8&parseTime=true"
		}
		if params == "-" {
			params = ""
		}
		dsn = fmt.Sprintf(
			"%s:%s@%s(%s:%s)/%s%s",
			user,
			pass,
			proto,
			host,
			port,
			db,
			params,
		)
	}
	return dsn
}

func main() {
	// Connect to MariaDB
	if len(os.Args) < 2 {
		fmt.Printf("Arguments required: apiName 'Organization Name' 'domain'\n")
		fmt.Printf("Defined APIs: setOrgDomain\n")
		return
	}
	dtStart := time.Now()
	var db *sql.DB
	dsn := getConnectString("SH_")
	db, err := sql.Open("mysql", dsn)
	fatalOnError(err)
	defer func() { fatalOnError(db.Close()) }()
	var info string
	switch os.Args[1] {
	case "setOrgDomain":
		info, err = setOrgDomain(db, os.Args[2:len(os.Args)])
	default:
		fatalf("unknown API: '%s'", os.Args[1])
	}
	if info != "" {
		fmt.Printf("info: %s\n", info)
	}
	fatalOnError(err)
	dtEnd := time.Now()
	fmt.Printf("Time(%s): %v\n", os.Args[0], dtEnd.Sub(dtStart))
}
