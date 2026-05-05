with open("mosaico.py", "r") as f:
    text = f.read()

text = text.replace(
"""                        tiles_catalogo,
                                ultimas_posicoes,""",
"""                        tiles_catalogo,
                        ultimas_posicoes,""")

text = text.replace(
"""                    tiles_catalogo,
                            ultimas_posicoes,""",
"""                    tiles_catalogo,
                    ultimas_posicoes,""")

# Wait, let's just make it robust:
import re
text = re.sub(r' +ultimas_posicoes,\n *cor_anterior', r'                        ultimas_posicoes,\n                        cor_anterior', text)

with open("mosaico.py", "w") as f:
    f.write(text)
