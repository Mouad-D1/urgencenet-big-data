"""Mise en forme commune des rapports PDF (reportlab, polices Windows Calibri et Consolas)."""
import io
import json
import re
from pathlib import Path
from xml.sax.saxutils import escape

from PIL import Image as PILImage
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    Image,
    KeepTogether,
    Paragraph,
    Preformatted,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

HERE = Path(__file__).resolve().parent
CAPTURES = HERE / "captures"
REPOSITORY = "https://github.com/Mouad-D1/urgencenet-big-data"

AUTHORS = "Mouad DIOUANE et Achraf MALKI"

FONTS = Path("C:/Windows/Fonts")
pdfmetrics.registerFont(TTFont("Body", FONTS / "calibri.ttf"))
pdfmetrics.registerFont(TTFont("Body-Bold", FONTS / "calibrib.ttf"))
pdfmetrics.registerFont(TTFont("Body-Italic", FONTS / "calibrii.ttf"))
pdfmetrics.registerFont(TTFont("Mono", FONTS / "consola.ttf"))
pdfmetrics.registerFontFamily("Body", normal="Body", bold="Body-Bold", italic="Body-Italic", boldItalic="Body-Bold")

INK = colors.HexColor("#1F2933")
ACCENT = colors.HexColor("#1F4E79")
MUTED = colors.HexColor("#5F6B76")
RULE = colors.HexColor("#C9D1D9")
SHADE = colors.HexColor("#F2F5F8")

BODY = ParagraphStyle("body", fontName="Body", fontSize=10.5, leading=13.4, textColor=INK, alignment=TA_JUSTIFY, spaceAfter=5)
TITLE = ParagraphStyle("title", fontName="Body-Bold", fontSize=19, leading=23, textColor=ACCENT, spaceAfter=2)
SUBTITLE = ParagraphStyle("subtitle", fontName="Body", fontSize=12.5, leading=16, textColor=INK, spaceAfter=4)
META = ParagraphStyle("meta", fontName="Body", fontSize=9.5, leading=12.5, textColor=MUTED, spaceAfter=8)
H1 = ParagraphStyle("h1", fontName="Body-Bold", fontSize=14, leading=17, textColor=ACCENT, spaceBefore=11, spaceAfter=5, keepWithNext=1)
H2 = ParagraphStyle("h2", fontName="Body-Bold", fontSize=11, leading=14, textColor=INK, spaceBefore=7, spaceAfter=3, keepWithNext=1)
BULLET = ParagraphStyle("bullet", parent=BODY, leftIndent=12, bulletIndent=2, spaceAfter=3)
CELL = ParagraphStyle("cell", fontName="Body", fontSize=9.5, leading=11.6, textColor=INK)
CELL_HEAD = ParagraphStyle("cellhead", parent=CELL, fontName="Body-Bold")
CAPTION = ParagraphStyle("caption", fontName="Body-Italic", fontSize=9, leading=11, textColor=MUTED, alignment=TA_CENTER, spaceBefore=2)
CODE = ParagraphStyle("code", fontName="Mono", fontSize=7.4, leading=9.2, textColor=INK)

CONTENT_WIDTH = A4[0] - 4 * cm


def markup(text):
    """Echappe le texte, puis applique `code`, **gras** et les espaces insecables du francais."""
    text = escape(text)
    text = re.sub(r"`([^`]+)`", r'<font name="Mono" size="9">\1</font>', text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", text)
    return re.sub(r" ([:;?!%])(?=[\s<]|$)", "\u00a0\\1", text)


def p(text, style=BODY):
    return Paragraph(markup(text), style)


def bullets(*items):
    return [Paragraph(markup(item), BULLET, bulletText="•") for item in items]


def code(text):
    table = Table([[Preformatted(text.strip("\n"), CODE)]], colWidths=[CONTENT_WIDTH])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), SHADE),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    return [table, Spacer(1, 6)]


def table(rows, widths):
    total = sum(widths)
    col_widths = [w / total * CONTENT_WIDTH for w in widths]
    cells = [[p(str(c), CELL_HEAD if i == 0 else CELL) for c in row] for i, row in enumerate(rows)]
    result = Table(cells, colWidths=col_widths, repeatRows=1, hAlign="LEFT")
    style = [
        ("LINEBELOW", (0, 0), (-1, 0), 0.8, ACCENT),
        ("LINEBELOW", (0, 1), (-1, -1), 0.3, RULE),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 2.2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2.8),
    ]
    result.setStyle(TableStyle(style))
    return [result, Spacer(1, 7)]


def picture(name, width, crop=None):
    """Capture a la largeur voulue, recadree sur crop (gauche, haut, droite, bas) en pixels si besoin."""
    with PILImage.open(CAPTURES / name) as image:
        selection = image.crop(crop) if crop else image.copy()
    buffer = io.BytesIO()
    selection.save(buffer, format="PNG")
    buffer.seek(0)
    result = Image(buffer, width=width, height=width * selection.height / selection.width)
    result.hAlign = "CENTER"
    return result


def figure(name, caption, width=CONTENT_WIDTH, crop=None):
    return KeepTogether([picture(name, width, crop), p(caption, CAPTION), Spacer(1, 6)])


def figures_side_by_side(left, right, left_share=0.5, left_crop=None, right_crop=None):
    """Deux captures cote a cote : left et right sont des couples (fichier, legende)."""
    gap = 0.4 * cm
    left_width = (CONTENT_WIDTH - gap) * left_share
    right_width = CONTENT_WIDTH - gap - left_width
    grid = Table(
        [[picture(left[0], left_width, left_crop), picture(right[0], right_width, right_crop)],
         [p(left[1], CAPTION), p(right[1], CAPTION)]],
        colWidths=[left_width + gap / 2, right_width + gap / 2],
    )
    grid.setStyle(TableStyle([
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("VALIGN", (0, 0), (-1, 0), "TOP"),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("TOPPADDING", (0, 0), (-1, -1), 1),
    ]))
    return KeepTogether([grid, Spacer(1, 6)])


def number(value):
    return f"{value:,}".replace(",", "\u00a0")


def decimal(value, digits=2):
    return f"{value:.{digits}f}".replace(".", ",")


def make_footer(label):
    """En-tete et pied de page de chaque page, avec le libelle du document."""

    def footer(canvas, doc):
        canvas.saveState()
        canvas.setFont("Body", 8.5)
        canvas.setFillColor(MUTED)
        canvas.drawString(2 * cm, A4[1] - 1.25 * cm, "Écosystème Big Data 2")
        canvas.drawRightString(A4[0] - 2 * cm, A4[1] - 1.25 * cm, "5ème année EMSI")
        canvas.drawString(2 * cm, 1.15 * cm, label)
        canvas.drawRightString(A4[0] - 2 * cm, 1.15 * cm, f"Page {doc.page}")
        canvas.restoreState()

    return footer
