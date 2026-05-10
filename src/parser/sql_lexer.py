import ply.lex as lex

# 1. Palabras reservadas según el PDF del proyecto
reserved = {
    'CREATE': 'CREATE',
    'TABLE': 'TABLE',
    'FROM': 'FROM',
    'FILE': 'FILE',
    'INDEX': 'INDEX',
    # Tipos de Datos
    'INT': 'INT',
    'FLOAT': 'FLOAT',
    'VARCHAR': 'VARCHAR',
    'STR': 'STR', 
    # Técnicas de Indexación 
    'BTREE': 'BTREE',
    'RTREE': 'RTREE',
    'HASH': 'HASH',
    'SEQUENTIAL': 'SEQUENTIAL',
 
    'SELECT': 'SELECT',
    'WHERE': 'WHERE',
    'BETWEEN': 'BETWEEN',
    'AND': 'AND',
    'IN': 'IN',
    'POINT': 'POINT',
    'RADIUS': 'RADIUS',
    'K': 'K',
    'INSERT': 'INSERT',
    'INTO': 'INTO',
    'VALUES': 'VALUES',
    'DELETE': 'DELETE',
    # Carga parametrizada 
    'WITH': 'WITH',
    'N': 'N',
    # Metadata / catálogo 
    'SHOW': 'SHOW',
    'TABLES': 'TABLES',
    'VIEW': 'VIEW',
    'INDICES': 'INDICES',
    'DROP': 'DROP',
}

# 2. Lista de todos los tokens
tokens = [
    'ID',
    'COMMA',
    'LPAREN',
    'RPAREN',
    'STRING',
    'ASTERISK',
    'EQUALS',
    'NUMBER',
    'SEMICOLON'
] + list(reserved.values())

# 3. Expresiones regulares simples
t_SEMICOLON = r';'
t_ASTERISK = r'\*'
t_EQUALS = r'='
t_COMMA  = r','
t_LPAREN = r'\('
t_RPAREN = r'\)'
t_ignore = ' \t\n' 

# 4. Regla para Strings (ej. 'ruta.csv' o "Farid")
def t_STRING(t):
    r'\'[^\']*\'|\"[^\"]*\"'
    t.value = t.value[1:-1]
    return t

# 5. Regla para Identificadores y Palabras Reservadas
def t_ID(t):
    r'[a-zA-Z_][a-zA-Z_0-9]*'
    t.type = reserved.get(t.value.upper(), 'ID')
    return t
class LexError(Exception):
    """Error léxico capturable por el SQLParser para mostrarlo en el UI."""
    pass


# 7. Manejo de errores
def t_error(t):
    raise LexError(f"carácter ilegal {t.value[0]!r} en línea "
                   f"{getattr(t, 'lineno', '?')}")

def t_NUMBER(t):
    r'-?\d+(\.\d+)?' 
    t.value = float(t.value) if '.' in t.value else int(t.value)
    return t
# Construir el analizador léxico
lexer = lex.lex()
