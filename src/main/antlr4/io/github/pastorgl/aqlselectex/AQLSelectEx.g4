grammar AQLSelectEx;

parse
 : ( select_stmt | error ) EOF
 ;

error
 : UNEXPECTED_CHAR
   {
     throw new RuntimeException("UNEXPECTED_CHAR=" + $UNEXPECTED_CHAR.text);
   }
 ;

select_stmt
 : K_SELECT ( STAR | column_name ( COMMA column_name )* )
   ( K_FROM from_set )?
   ( (K_USE | K_WITH) index_expr )?
   ( K_WHERE where_expr )?
 ;

type_name
 : K_DECIMAL | K_INT | K_NUMERIC
 | K_FLOAT | K_REAL
 | K_JSON
 | K_LIST
 | K_MAP
 | K_GEOJSON
 | K_CHAR | K_STRING | K_TEXT | K_VARCHAR
 ;

where_expr
 : ( atomic_expr | OPEN_PAR | CLOSE_PAR | logic_op )+
 ;

logic_op
 : K_NOT | K_AND | K_OR
 ;

atomic_expr
 : column_name ( equality_op | regex_op ) STRING_LITERAL
 | ( column_name | meta_name ) ( equality_op | comparison_op ) NUMERIC_LITERAL
 | column_name map_op iter_expr
 | column_name list_op iter_expr
 | column_name geo_op cast_expr
 ;

equality_op
 : EQ | EQ2 | NOT_EQ1 | NOT_EQ2
 ;

comparison_op
 : LT | LT_EQ | GT | GT_EQ
 ;

map_op
 : K_ANY? K_MAPVALUES | K_ANY? K_MAPKEYS
 ;

iter_expr
 : OPEN_PAR var_name equality_op STRING_LITERAL CLOSE_PAR
 | OPEN_PAR var_name ( equality_op | comparison_op ) NUMERIC_LITERAL CLOSE_PAR
 ;

geo_op
 : K_CONTAINS | K_WITHIN
 ;

list_op
 : K_ANY? K_CONTAINS
 ;

regex_op
 : K_LIKE | K_MATCH | K_REGEXP
 ;

index_expr
 : index_type? OPEN_PAR (
 column_name index_op ( NUMERIC_LITERAL | STRING_LITERAL )
 | column_name geo_op cast_expr
 | column_name between_expr )
 CLOSE_PAR
 ;

index_op
 : EQ | EQ2 | K_CONTAINS
 ;

between_expr
 : K_BETWEEN low=NUMERIC_LITERAL K_AND high=NUMERIC_LITERAL
 ;

cast_expr
 : K_CAST OPEN_PAR STRING_LITERAL K_AS type_name CLOSE_PAR
 | type_name OPEN_PAR STRING_LITERAL CLOSE_PAR
 ;

from_set
 : ns_name ( DOT set_name )?
 ;

index_type
 : K_DEFAULT
 | K_LIST
 | K_MAPKEYS
 | K_MAPVALUES
 ;

column_name
 : bin_name
 ;

meta_name
 : K_LUT
 | K_RECSIZE
 | K_TTL
 | K_DIGEST OPEN_PAR NUMERIC_LITERAL CLOSE_PAR
 ;

ns_name
 : IDENTIFIER
 ;

set_name
 : IDENTIFIER
 ;

bin_name
 : K_PK | IDENTIFIER
 ;

var_name
 : IDENTIFIER
 ;

SCOL : ';';
DOT : '.';
OPEN_PAR : '(';
CLOSE_PAR : ')';
COMMA : ',';
EQ : '=';
STAR : '*';
PLUS : '+';
MINUS : '-';
LT : '<';
LT_EQ : '<=';
GT : '>';
GT_EQ : '>=';
EQ2 : '==';
NOT_EQ1 : '!=';
NOT_EQ2 : '<>';

K_AND : A N D;
K_ANY : A N Y;
K_AS : A S;
K_BETWEEN : B E T W E E N;
K_CAST : C A S T;
K_CHAR : C H A R;
K_CONTAINS : C O N T A I N S;
K_DECIMAL : D E C I M A L;
K_DEFAULT : D E F A U L T;
K_DIGEST : D I G E S T;
K_FLOAT : F L O A T;
K_FROM : F R O M;
K_GEOJSON : G E O J S O N;
K_INT : I N T;
K_JSON : J S O N;
K_LIKE : L I K E;
K_LIST : L I S T;
K_LUT : L U T;
K_MATCH : M A T C H;
K_MAP : M A P;
K_MAPKEYS : M A P K E Y S;
K_MAPVALUES : M A P V A L U E S;
K_NOT : N O T;
K_NUMERIC : N U M E R I C;
K_OR : O R;
K_PK : P K;
K_REAL : R E A L;
K_RECSIZE : R E C S I Z E;
K_REGEXP : R E G E X P;
K_SELECT : S E L E C T;
K_STRING : S T R I N G;
K_TEXT : T E X T;
K_TTL : T T L;
K_USE : U S E;
K_VARCHAR : V A R C H A R;
K_WHERE : W H E R E;
K_WITH : W I T H;
K_WITHIN : W I T H I N;

IDENTIFIER
 : '"' (~'"' | '""')* '"'
 | [a-zA-Z_] [a-zA-Z_0-9]*
 ;

UNARY_OPERATOR
 : PLUS
 | MINUS
 ;

NUMERIC_LITERAL
 : UNARY_OPERATOR? DIGIT+ ( DOT DIGIT* )? ( E UNARY_OPERATOR? DIGIT+ )?
 | UNARY_OPERATOR DOT DIGIT+ ( E UNARY_OPERATOR? DIGIT+ )?
 ;

INTEGER_LITERAL
 : UNARY_OPERATOR? DIGIT+ L?
 ;

APOS
 : '\''
 ;

STRING_LITERAL
 : APOS ( ~'\'' | '\'\'' )* APOS
 ;

SPACES
 : [ \u000B\t\r\n] -> channel(HIDDEN)
 ;

UNEXPECTED_CHAR
 : .
 ;

fragment DIGIT : [0-9];

fragment A : [aA];
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];