import re
text="MELANNCO Portrait Frame (Black, 16x20-Inch/11x14-Inch)"
brand="Mr. Coffee"
for color in ["Amaranth", "Amber", "Amethyst", "Aquamarine", "Azure", "Baby blue", "Beige", "Black", "Blue",
              "Blue-green", "Blue-violet", "Blush", "Bronze", "Brown", "Burgundy", "Byzantium", "Carmine", "Cerise",
              "Cerulean", "Chartreuse green", "Cobalt blue", "Coral", "Crimson", "Cyan", "Desert sand", "Electric blue",
              "Emerald", "Erin", "Gold", "Gray", "Green", "Harlequin", "Indigo", "Ivory", "Jade", "Jungle green",
              "Lavender", "Lemon", "Lilac", "Lime", "Magenta", "Magenta rose", "Maroon", "Mauve", "Navy blue", "Ocher",
              "Olive", "Orange", "Orange-red", "Orchid", "Peach", "Pear", "Periwinkle", "Persian blue", "Pink", "Plum",
              "Prussian blu", "Puce", "Purple", "Raspberry", "Red", "Red-violet", "Rose", "Ruby", "Sangria", "Scarlet",
              "Silver", "Slate gray", "Spring bud", "Spring green", "Tan", "Taupe", "Teal", "Turquoise", "Violet",
              "Viridian", "White", "Yellow", "Inch", "Foot", "Pack", "Ml", "grams", "gram", "inch", "amount", "breadth",
              "capacity", "content", "diameter", "extent", "height", "intensity", "length", "magnitude", "proportion",
              "range", "scope", "stature", "volume", "width", "amplitude", "area", "bigness", "caliber",
              "capaciousness", "dimensions", "enormity", "extension", "greatness", "highness", "immensity", "largeness",
              "proportions", "substantiality", "vastness", "voluminosity", "admeasurement", "hugeness", "burden",
              "heft", "load", "pressure", "substance", "adiposity", "avoirdupois", "ballast", "gross", "heftiness",
              "mass", "measurement", "net", "ponderosity", "ponderousness", "poundage", "tonnage", "G-factor"]:
    if brand.lower() not in ["mass", "g-factor", "gravity"] and str(color).lower() not in ["mass", "g-factor", "gravity"]:
        # text = re.sub(r"\s+" + str(color).lower() + "\s+|\s+" + str(color).lower() + "$|^" + str(color).lower() + "\s+|", str(text).lower(), re.I)
        # print(text)
        # regex="[\(\)]" + str(color) + "[\(\)]"
        regex="[\(\),/\s-]*"+str(color).lower()+"[-\(\),/\s]*"
        print(regex)
        text = re.sub(str(regex)," ", str(text).lower(), re.I)
        print(text)
        text = re.sub(r"[\-./,;%#@$!\d\'\(\)\"]+", " ", str(text), re.I)
        text = re.sub("\s+", " ", str(text).lower(), re.I)
        # text_src = re.sub(
        #     r"\s+" + str(color).lower() + "\s+|\s+" + str(color).lower() + "$|^" + str(color).lower() + "\s+", "",
        #     str(text_src).lower(), re.I)
        # text_src = re.sub(r"[\-./,;%#@$!\d\'\"]+", "", str(text_src), re.I)
text_len_1 = len(text)
print(text)