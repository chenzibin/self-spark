grammar Calculator;

line : expr EOF;
expr : '(' expr ')'             # parenExpr
    | expr ('*' | '/') expr     # multOrDiv
    | expr ('+' | '-') expr     # addOrSubtract
    | FLOAT                     # float
