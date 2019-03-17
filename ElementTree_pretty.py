from xml.etree import ElementTree
from xml.dom import minidom

def prettify(elem):
    """Return a pretty-printed XML string for the Element.
    """
    rough_string = ElementTree.tostring(elem, encoding='utf-8', method='xml', short_empty_elements=True)
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")
