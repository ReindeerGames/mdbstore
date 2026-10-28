package schema

import "testing"

func TestSplitSQLStatements(t *testing.T) {
	got := splitSQLStatements("-- comment\nCREATE TABLE a (value VARCHAR(20) DEFAULT ';'); /* ; */\n# comment\nCREATE TABLE `b;c` (id INT);\n")
	if len(got) != 2 {
		t.Fatalf("got %d statements: %#v", len(got), got)
	}
}

func TestSplitSQLStatementsRejectsUnterminatedInput(t *testing.T) {
	if got := splitSQLStatements("SELECT 'unterminated;"); got != nil {
		t.Fatalf("got %#v", got)
	}
}
