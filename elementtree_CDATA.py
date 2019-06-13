import xml.etree.ElementTree as ET


def CDATA(text=None):
    element = ET.Element('![CDATA[')
    element.text = text
    return element

ET._original_serialize_xml = ET._serialize_xml


def _serialize_xml(write, elem, encoding, qnames, namespaces):
    if elem.tag == '![CDATA[':
        write("<%s%s]]>%s" % (elem.tag, elem.text, elem.tail))
        return
    return ET._original_serialize_xml(
        write, elem, encoding, qnames, namespaces)
        
ET._serialize_xml = ET._serialize['xml'] = _serialize_xml


# usage
if __name__ == "__main__":
    import sys

    text = """
    <?xml version='1.0' encoding='utf-8'?>
    <text>
    This is just some sample text.
    </text>
    """

    e = ET.Element("data")
    cdata = CDATA(text)
    e.append(cdata)
    et = ET.ElementTree(e)
    et.write(sys.stdout, "utf-8")