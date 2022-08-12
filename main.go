package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/alexflint/go-arg"
	"github.com/huandu/go-sqlbuilder"
	"github.com/kr/pretty"
	"golang.org/x/exp/maps"
)

type args struct {
	CSVPath         string   `arg:"positional,required" placeholder:"CSV"`
	CsvPK           string   `arg:"--pk,required"`
	Table           string   `arg:"-t,required"`
	Columns         []string `arg:"-c,required" help:"to provide an alias in the output sql, use the format \"csvcol->sqlcol\""`
	ValueTransforms []string `arg:"-f" help:"transform values: \"csvval->sqlval\""`
	Verbose         bool     `arg:"-v"`
}

func (args) Description() string {
	return "Converts a CSV file to a set of SQL updates"
}

type transform struct {
	CSV string
	SQL string
}

func newTransform(tfstring string) (tf transform, err error) {
	names := strings.Split(tfstring, "->")
	switch len(names) {
	case 1:
		return transform{names[0], names[0]}, nil
	case 2:
		return transform{names[0], names[1]}, nil
	default:
		return tf, fmt.Errorf("at most two \"->\" allowed; got %q", tfstring)
	}
}

func transforms(tfstrings []string) (transforms []transform, err error) {
	transforms = []transform{}
	for _, tfstring := range tfstrings {
		tf, err := newTransform(tfstring)
		if err != nil {
			return transforms, err
		}
		transforms = append(transforms, tf)
	}
	return transforms, nil
}

type column transform

func newColumn(colstring string) (column, error) {
	tf, err := newTransform(colstring)
	return column(tf), err
}

func columns(colstrings []string) (cols []column, err error) {
	tfs, err := transforms(colstrings)
	if err != nil {
		return cols, err
	}
	for _, tf := range tfs {
		cols = append(cols, column(tf))
	}
	return cols, nil
}

type record map[string]string

func head[T any](lines []T, n int) []T {
	end := n
	if len(lines) < n {
		end = len(lines)
	}
	return lines[:end]
}

func csvRecords(csvPath string) (records []record, err error) {
	records = []record{}

	f, err := os.Open(csvPath)
	if err != nil {
		return records, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	lines, err := reader.ReadAll()
	if err != nil {
		return records, err
	}

	csvHeaders := lines[0]
	for _, line := range lines[1:] {
		record := map[string]string{}
		for i, header := range csvHeaders {
			record[header] = line[i]
		}
		records = append(records, record)
	}

	return records, nil
}

func sqlRecords(csvRecords []record, columns []column, valTransforms []transform) (sqlRecords []record, err error) {
	sqlRecords = []record{}
	for _, csvRecord := range csvRecords {
		sqlRecord := record{}
		for csvCol, csvVal := range csvRecord {
			for _, col := range columns {
				if csvCol == col.CSV {
					v := csvVal
					for _, val := range valTransforms {
						if csvVal == val.CSV {
							v = val.SQL
						}
					}
					sqlRecord[col.SQL] = v
				}
			}
		}
		sqlRecords = append(sqlRecords, sqlRecord)
	}
	return sqlRecords, nil
}

func updateQueries(updates []record, table string, pk column) (queries []string, err error) {
	intif := func(v string) any {
		intVal, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return v
		}
		return intVal
	}

	cols := maps.Keys(updates[0])
	sort.Strings(cols)
	for _, upd := range updates {
		var pkVal string
		ub := sqlbuilder.MySQL.NewUpdateBuilder()
		ub.Update(table)
		assigns := []string{}
		for _, col := range cols {
			v := upd[col]
			switch {
			case col == pk.SQL:
				pkVal = v
			case v == "":
				assigns = append(assigns, ub.Assign(col, nil))
			case v == "now()":
				assigns = append(assigns, ub.Assign(col, sqlbuilder.Raw(v)))
			default:
				assigns = append(assigns, ub.Assign(col, intif(v)))
			}
		}
		ub.Set(assigns...)
		ub.Where(ub.Equal(pk.SQL, intif(pkVal)))
		sql, args := ub.Build()
		query, err := sqlbuilder.MySQL.Interpolate(sql, args)
		if err != nil {
			return queries, err
		}
		queries = append(queries, query)
	}
	return queries, nil
}

type updateCmd struct {
	args    args
	verbose bool
}

func (cmd *updateCmd) logV(format string, a ...any) {
	if !cmd.verbose {
		return
	}
	fmt.Printf(format, a...)
}

func (cmd *updateCmd) debug(msg string, v any) {
	if !cmd.verbose {
		return
	}
	fmt.Printf("\n\033[1m%s:\033[0m %# v\n", msg, pretty.Formatter(v))
}

func (cmd *updateCmd) run() (err error) {
	cmd.debug("args", cmd.args)

	pk, err := newColumn(cmd.args.CsvPK)
	if err != nil {
		return err
	}
	cmd.debug("pk column", pk)

	cols, err := columns(cmd.args.Columns)
	if err != nil {
		return err
	}
	cmd.debug("columns", cols)

	valTransforms, err := transforms(cmd.args.ValueTransforms)
	if err != nil {
		return err
	}
	cmd.debug("value transforms", valTransforms)

	csv, err := csvRecords(cmd.args.CSVPath)
	if err != nil {
		return err
	}
	cmd.debug("csv", head(csv, 5))
	cmd.logV("...\n(%v records)\n", len(csv))

	cols = append(cols, pk)
	updates, err := sqlRecords(csv, cols, valTransforms)
	if err != nil {
		return err
	}
	sort.Slice(updates, func(i, j int) bool {
		return updates[i][pk.SQL] < updates[j][pk.SQL]
	})
	cmd.debug("updates", head(updates, 5))
	cmd.logV("...\n(%v records)\n", len(updates))

	stmts, err := updateQueries(updates, cmd.args.Table, pk)
	sql := strings.Join(stmts, ";\n") + ";"
	fmt.Println(sql)
	return nil
}

func main() {
	var args args
	arg.MustParse(&args)
	app := updateCmd{
		args:    args,
		verbose: args.Verbose,
	}
	if err := app.run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
