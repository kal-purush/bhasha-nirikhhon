import math

def polar_to_cartesian(radius, angle_degrees):
    angle_radians = math.radians(angle_degrees)
    x = radius * math.cos(angle_radians)
    y = radius * math.sin(angle_radians)
    return x, y

def generate_svg_arcs(books, radius=100):
    total_words = sum(books.values())
    start_angle = 0
    arc_paths = []
    
    for book, word_count in books.items():
        sweep_angle = (word_count / total_words) * 360
        end_angle = start_angle + sweep_angle
        
        x1, y1 = polar_to_cartesian(radius, start_angle)
        x2, y2 = polar_to_cartesian(radius, end_angle)
        large_arc_flag = 1 if sweep_angle > 180 else 0
        
        path = (f'<path id="{book}" d="M {x1:.2f} {y1:.2f} A {radius} {radius} 0 {large_arc_flag} 1 '
                f'{x2:.2f} {y2:.2f}" stroke="black" stroke-width="2" fill="none"/>')
        arc_paths.append(path)
        
        start_angle = end_angle
    
    return "\n".join(arc_paths)

# Example book data (replace with actual word counts)
bible_books = {
    "Genesis": 38262, "Exodus": 32685, "Leviticus": 24541, "Numbers": 32896, "Deuteronomy": 28352,
    "Joshua": 18854, "Judges": 18966, "Ruth": 2574, "1 Samuel": 25048, "2 Samuel": 20600,
    "1 Kings": 24513, "2 Kings": 23517, "1 Chronicles": 20365, "2 Chronicles": 26069, "Ezra": 7440,
    "Nehemiah": 10480, "Esther": 5633, "Job": 10702, "Psalms": 42704, "Proverbs": 15038,
    "Ecclesiastes": 5579, "Song of Solomon": 2658, "Isaiah": 37036, "Jeremiah": 42654, "Lamentations": 3411,
    "Ezekiel": 39401, "Daniel": 11602, "Hosea": 5174, "Joel": 2033, "Amos": 4216, "Obadiah": 669,
    "Jonah": 1320, "Micah": 3152, "Nahum": 1284, "Habakkuk": 1475, "Zephaniah": 1616, "Haggai": 1131,
    "Zechariah": 6443, "Malachi": 1781, "Matthew": 23143, "Mark": 14949, "Luke": 25640, "John": 18658,
    "Acts": 24229, "Romans": 9467, "1 Corinthians": 9462, "2 Corinthians": 6046, "Galatians": 3084,
    "Ephesians": 3022, "Philippians": 2183, "Colossians": 1979, "1 Thessalonians": 1837, "2 Thessalonians": 1022,
    "1 Timothy": 2244, "2 Timothy": 1666, "Titus": 896, "Philemon": 430, "Hebrews": 4953,
    "James": 2304, "1 Peter": 2483, "2 Peter": 1553, "1 John": 2517, "2 John": 298, "3 John": 294,
    "Jude": 608, "Revelation": 11952
}

svg_arcs = generate_svg_arcs(bible_books)
print(svg_arcs)