import re

with open("mosaico.py", "r") as f:
    text = f.read()

# remove usos_globais from selecionar_pixel
text = re.sub(r'    usos_globais: dict\[str, int\] \| None = None,\n', '', text)
text = re.sub(r'        usos_globais: Dict com contagem de usos por tile\n', '', text)
text = re.sub(r'    usos_globais = usos_globais or \{\}\n', '', text)
text = re.sub(r'usos = usos_globais\.get\(pixel\.path, pixel\.uses\)', 'usos = pixel.uses', text)
text = re.sub(r'usos_globais\.get\(pixel\.path, pixel\.uses\)', 'pixel.uses', text)

# remove usos_globais from _renderizar_faixa
text = re.sub(r'    usos_globais: Dict\[str, int\],\n', '', text)
text = re.sub(r'                usos_globais,\n', '', text)

# fix the assignment inside _renderizar_faixa
text = re.sub(
    r'            usos_globais\[pixel_selecionado\.path\] = usos_globais\.get\(pixel_selecionado\.path, pixel_selecionado\.uses\) \+ 1\n            pixel_selecionado\.uses = usos_globais\[pixel_selecionado\.path\]',
    '            pixel_selecionado.uses += 1',
    tex    tex    tex    tex    tex  m criar_mosaico
text = re.text = re.text = re.text = re.text = re] = \text = re.text = re.text = re.text = re.text = re] = \text = re.text = re.tnt("Done")
