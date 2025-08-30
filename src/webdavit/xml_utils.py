"""XML utilities for WebDAV."""

from typing import Any

from lxml import etree  # type: ignore

from .exceptions import WebDAVParseError

# WebDAV namespaces
DAV_NAMESPACE = "DAV:"
XMLNS_DAV = {"D": DAV_NAMESPACE}


def parse_status(status: str) -> int:
    """
    Parse HTTP status line to extract the status code.

    Arguments:
        status: The HTTP status line (e.g., "HTTP/1.1 200 OK").

    Returns:
        The HTTP status code as an integer.

    Raises:
        ValueError: The status line is malformed or does not contain a valid status code.
    """
    parts = status.split()
    if len(parts) < 2:
        raise ValueError(f"Malformed status line: {status}")
    if not parts[0].startswith("HTTP/"):
        raise ValueError(f"Invalid status line: {status}")
    try:
        return int(parts[1])
    except ValueError as e:
        raise ValueError(f"Invalid status code in line: {status}") from e


def create_propfind_request(properties: list[str]) -> bytes:
    """
    Create PROPFIND request XML.

    Arguments:
        properties:
            List of property names to include in the request.
            XML namespaces are required for all properties.

    Returns:
        The PROPFIND request XML as bytes.

    Raises:
        None
    """
    # Example PROPFIND request from Nextcloud:
    # <?xml version="1.0"?>
    # <d:propfind xmlns:d="DAV:"
    #     xmlns:nc="http://nextcloud.org/ns"
    #     xmlns:oc="http://owncloud.org/ns"
    #     xmlns:ocs="http://open-collaboration-services.org/ns">
    #   <d:prop>
    # <d:getcontentlength />
    #     <d:getcontenttype />
    #     <d:getetag />
    #     <d:getlastmodified />
    #     <d:creationdate />
    #     <d:displayname />
    #     <d:quota-available-bytes />
    #     <d:resourcetype />
    #     <nc:has-preview />
    #     <nc:is-encrypted />
    #     <nc:mount-type />
    #     <oc:comments-unread />
    #     <oc:favorite />
    #     <oc:fileid />
    #     <oc:owner-display-name />
    #     <oc:owner-id />
    #     <oc:permissions />
    #     <oc:size />
    #     <nc:hidden />
    #     <nc:is-mount-root />
    #     <nc:metadata-blurhash />
    #     <nc:metadata-files-live-photo />
    #     <nc:note />
    #     <nc:sharees />
    #     <nc:hide-download />
    #     <nc:share-attributes />
    #     <oc:share-types />
    #     <ocs:share-permissions />
    #     <nc:rich-workspace />
    #     <nc:rich-workspace-file />
    #   </d:prop>
    # </d:propfind>
    root = etree.Element(f"{{{DAV_NAMESPACE}}}propfind", nsmap={None: DAV_NAMESPACE})
    prop = etree.SubElement(root, f"{{{DAV_NAMESPACE}}}prop")
    for property_name in properties:
        etree.SubElement(prop, property_name)
    return etree.tostring(root, xml_declaration=True, encoding="utf-8")


def parse_propfind_result(xml_content: bytes) -> dict[str, dict]:
    """
    Parse PROPFIND multistatus response.

    Arguments:
        xml_content: The multistatus XML content as bytes.

    Returns:
        A dictionary with hrefs as keys and property dictionaries as values.
        Each property dictionary contains property names as keys and dictionaries with
        'value', 'status', and 'description' as keys.

    Raises:
        WebDAVParseError: The XML could not be parsed or is malformed.
    """
    # Example PROPFIND response from Nextcloud:
    # <?xml version="1.0"?>
    # <d:multistatus xmlns:d="DAV:"
    #     xmlns:s="http://sabredav.org/ns"
    #     xmlns:oc="http://owncloud.org/ns"
    #     xmlns:nc="http://nextcloud.org/ns">
    #   <d:response>
    #     <d:href>/remote.php/dav/files/admin/Prism/Organization%20Documents/testing/</d:href>
    #     <d:propstat>
    #       <d:prop>
    #         <d:getetag>&quot;68b221c9268f1&quot;</d:getetag>
    #         <d:getlastmodified>Fri, 29 Aug 2025 21:55:21 GMT</d:getlastmodified>
    #         <d:creationdate>1970-01-01T00:00:00+00:00</d:creationdate>
    #         <d:displayname>testing</d:displayname>
    #         <d:quota-available-bytes>-3</d:quota-available-bytes>
    #         <d:resourcetype>
    #           <d:collection/>
    #         </d:resourcetype>
    #         <nc:has-preview>false</nc:has-preview>
    #         <nc:mount-type>group</nc:mount-type>
    #         <oc:comments-unread>0</oc:comments-unread>
    #         <oc:favorite>0</oc:favorite>
    #         <oc:fileid>1599</oc:fileid>
    #         <oc:owner-display-name>admin</oc:owner-display-name>
    #         <oc:owner-id>admin</oc:owner-id>
    #         <oc:permissions>RMGDNVCK</oc:permissions>
    #         <oc:size>416424</oc:size>
    #         <nc:hidden>false</nc:hidden>
    #         <nc:is-mount-root>false</nc:is-mount-root>
    #         <nc:sharees/>
    #         <nc:share-attributes>[]</nc:share-attributes>
    #         <oc:share-types/>
    #         <x1:share-permissions xmlns:x1="http://open-collaboration-services.org/ns">31</x1:share-permissions>
    #         <nc:rich-workspace></nc:rich-workspace>
    #         <nc:rich-workspace-file></nc:rich-workspace-file>
    #       </d:prop>
    #       <d:status>HTTP/1.1 200 OK</d:status>
    #     </d:propstat>
    #   </d:response>
    # </d:multistatus>
    try:
        root = etree.fromstring(xml_content)
    except etree.XMLSyntaxError as e:
        raise WebDAVParseError(
            f"Failed to parse multistatus XML response: {e}\nContent: {xml_content}"
        ) from e
    result = {}
    description_el = root.find("D:responsedescription", XMLNS_DAV)
    if description_el is not None and description_el.text:
        description = description_el.text
    else:
        description = ""
    responses = root.xpath("//D:response", namespaces=XMLNS_DAV)
    for response in responses:
        href_elem = response.find("D:href", XMLNS_DAV)
        if href_elem is None:
            raise WebDAVParseError("Missing <D:href> in multistatus response")
        href = href_elem.text
        if not href:
            raise WebDAVParseError("Empty <D:href> in multistatus response")
        if href in result:
            raise WebDAVParseError(f"Duplicate <D:href> found: {href}")
        result[href] = this_result = {}
        propstats = response.findall("D:propstat", XMLNS_DAV)
        for propstat in propstats:
            status_el = propstat.find("D:status", XMLNS_DAV)
            if status_el is None:
                raise WebDAVParseError("Missing <D:status> in <D:propstat>")
            status = parse_status(status_el.text or "HTTP/1.1 204 No Content")
            prop_elem = propstat.find("D:prop", XMLNS_DAV)
            if prop_elem is None:
                continue
            for prop in prop_elem:
                prop_name = prop.tag
                if len(prop) > 0:
                    prop_value = prop
                else:
                    prop_value = prop.text or ""
                this_result[prop_name] = {
                    "value": prop_value,
                    "status": status,
                    "description": description,
                }
    return result


def parse_move_copy_multistatus_response(xml_content: bytes) -> dict[str, dict]:
    """
    Parse MOVE/COPY multistatus response.

    Arguments:
        xml_content: The multistatus XML content as bytes.

    Returns:
        A dictionary with hrefs as keys and dictionaries with 'status' and 'description' as values.

    Raises:
        WebDAVParseError: The XML could not be parsed or is malformed.
    """
    # Example multistatus response from RFC 4918:
    # <?xml version="1.0" encoding="utf-8" ?>
    # <d:multistatus xmlns:d='DAV:'>
    #   <d:response>
    #     <d:href>http://www.example.com/othercontainer/C2/</d:href>
    #     <d:status>HTTP/1.1 423 Locked</d:status>
    #     <d:error><d:lock-token-submitted/></d:error>
    #   </d:response>
    # </d:multistatus>
    try:
        root = etree.fromstring(xml_content)
    except etree.XMLSyntaxError as e:
        raise WebDAVParseError(
            f"Failed to parse multistatus XML response: {e}\nContent: {xml_content}"
        ) from e
    result = {}
    description_el = root.xpath("//D:responsedescription", XMLNS_DAV)
    if description_el is not None and description_el.text:
        description = description_el.text
    else:
        description = ""
    responses = root.xpath("//D:response", namespaces=XMLNS_DAV)
    for response in responses:
        href_elem = response.find("D:href", XMLNS_DAV)
        if href_elem is None:
            raise WebDAVParseError("Missing <D:href> in multistatus response")
        href = href_elem.text
        if not href:
            raise WebDAVParseError("Empty <D:href> in multistatus response")
        if href in result:
            raise WebDAVParseError(f"Duplicate <D:href> found: {href}")
        status_el = response.find("D:status", XMLNS_DAV)
        if status_el is None:
            raise WebDAVParseError("Missing <D:status> in <D:response>")
        status = parse_status(status_el.text or "HTTP/1.1 204 No Content")
        result[href] = {"status": status, "description": description}
    return result


def create_lock_xml(owner: str | None, scope: str, type_: str) -> bytes:
    """
    Create LOCK request XML.

    Arguments:
        owner: The owner of the lock or None if not specified.
        scope: The lock scope, either "exclusive" or "shared".
        type_: The lock type, typically "write".

    Returns:
        The LOCK request XML as bytes.

    Raises:
        ValueError: If the scope or type_ is invalid.
    """
    # Example LOCK request from RFC 4918:
    # <?xml version="1.0" encoding="utf-8" ?>
    # <D:lockinfo xmlns:D='DAV:'>
    #   <D:lockscope><D:exclusive/></D:lockscope>
    #   <D:locktype><D:write/></D:locktype>
    #   <D:owner>
    #     <D:href>http://example.org/~ejw/contact.html</D:href>
    #   </D:owner>
    # </D:lockinfo>
    root = etree.Element(f"{{{DAV_NAMESPACE}}}lockinfo", nsmap={None: DAV_NAMESPACE})
    lockscope = etree.SubElement(root, f"{{{DAV_NAMESPACE}}}lockscope")
    if scope == "exclusive":
        etree.SubElement(lockscope, f"{{{DAV_NAMESPACE}}}exclusive")
    elif scope == "shared":
        etree.SubElement(lockscope, f"{{{DAV_NAMESPACE}}}shared")
    else:
        raise ValueError("Invalid lock scope. Must be 'exclusive' or 'shared'.")
    locktype = etree.SubElement(root, f"{{{DAV_NAMESPACE}}}locktype")
    if type_ == "write":
        etree.SubElement(locktype, f"{{{DAV_NAMESPACE}}}write")
    else:
        raise ValueError("Invalid lock type. Currently, only 'write' is supported.")
    if owner:
        owner_elem = etree.SubElement(root, f"{{{DAV_NAMESPACE}}}owner")
        href_elem = etree.SubElement(owner_elem, f"{{{DAV_NAMESPACE}}}href")
        href_elem.text = owner
    return etree.tostring(root, xml_declaration=True, encoding="utf-8")


def parse_lock_response(xml_content: bytes) -> dict[str, Any]:
    """
    Parse LOCK response XML.

    Arguments:
        xml_content: The LOCK response XML content as bytes.

    Returns:
        A dictionary with lock information, including 'locktoken', 'timeout', 'scope', and 'type'.

    Raises:
        WebDAVParseError: The XML could not be parsed or is malformed.
    """
    # Example LOCK response from RFC 4918:
    # <?xml version="1.0" encoding="utf-8" ?>
    # <D:prop xmlns:D="DAV:">
    #   <D:lockdiscovery>
    #     <D:activelock>
    #       <D:locktype><D:write/></D:locktype>
    #       <D:lockscope><D:exclusive/></D:lockscope>
    #       <D:depth>infinity</D:depth>
    #       <D:owner>
    #         <D:href>http://example.org/~ejw/contact.html</D:href>
    #       </D:owner>
    #       <D:timeout>Second-604800</D:timeout>
    #       <D:locktoken>
    #         <D:href
    #         >urn:uuid:e71d4fae-5dec-22d6-fea5-00a0c91e6be4</D:href>
    #       </D:locktoken>
    #       <D:lockroot>
    #         <D:href
    #         >http://example.com/workspace/webdav/proposal.doc</D:href>
    #       </D:lockroot>
    #     </D:activelock>
    #   </D:lockdiscovery>
    # </D:prop>
    try:
        root = etree.fromstring(xml_content)
    except etree.XMLSyntaxError as e:
        raise WebDAVParseError(
            f"Failed to parse LOCK response XML: {e}\nContent: {xml_content}"
        ) from e
    result = {}
    if root.tag != f"{{{DAV_NAMESPACE}}}prop":
        raise WebDAVParseError(
            "Invalid LOCK response XML: missing <D:prop> root element"
        )
    lockdiscovery = root.find(".//D:lockdiscovery", XMLNS_DAV)
    if lockdiscovery is not None:
        activelock = lockdiscovery.find("D:activelock", XMLNS_DAV)
        if activelock is not None:
            # Lock token
            locktoken_elem = activelock.find("D:locktoken", XMLNS_DAV)
            if locktoken_elem is not None:
                href_elem = locktoken_elem.find("D:href", XMLNS_DAV)
                if href_elem is not None:
                    result["locktoken"] = href_elem.text

            # Timeout
            timeout_elem = activelock.find("D:timeout", XMLNS_DAV)
            if timeout_elem is not None:
                result["timeout"] = timeout_elem.text

            # Lock scope
            lockscope_elem = activelock.find("D:lockscope", XMLNS_DAV)
            if lockscope_elem is not None:
                if lockscope_elem.find("D:exclusive", XMLNS_DAV) is not None:
                    result["scope"] = "exclusive"
                elif lockscope_elem.find("D:shared", XMLNS_DAV) is not None:
                    result["scope"] = "shared"

            # Lock type
            locktype_elem = activelock.find("D:locktype", XMLNS_DAV)
            if locktype_elem is not None:
                if locktype_elem.find("D:write", XMLNS_DAV) is not None:
                    result["type"] = "write"

            # Owner
            owner_elem = activelock.find("D:owner", XMLNS_DAV)
            if owner_elem is not None:
                href_elem = owner_elem.find("D:href", XMLNS_DAV)
                if href_elem is not None:
                    result["owner"] = href_elem.text

            # Lock root
            lockroot_elem = activelock.find("D:lockroot", XMLNS_DAV)
            if lockroot_elem is not None:
                href_elem = lockroot_elem.find("D:href", XMLNS_DAV)
                if href_elem is not None:
                    result["lockroot"] = href_elem.text

            # Depth
            depth_elem = activelock.find("D:depth", XMLNS_DAV)
            if depth_elem is not None:
                result["depth"] = depth_elem.text
    return result


def get_error_details(xml_content: bytes) -> str:
    """
    Parse WebDAV error response XML to extract error information.

    Arguments:
        xml_content: The error response XML content as bytes.

    Returns: A formatted string containing error details.

    Raises:
        WebDAVParseError: The XML could not be parsed.
    """
    # Example error response from Nextcloud:
    # <d:error xmlns:d="DAV:" xmlns:s="http://sabredav.org/ns">
    #    <s:exception>Sabre\DAV\Exception\NotFound</s:exception>
    #    <s:message>File with name //remote.php could not be located</s:message>
    # </d:error>
    if not xml_content.strip():
        return "No additional error information provided."
    try:
        root = etree.fromstring(xml_content)
    except etree.XMLSyntaxError:
        return f"Error information (non-XML): {xml_content.decode('utf-8', errors='ignore').strip()}"
    return "Error information:\n" + "\n".join(
        f"    {elem.tag}: {elem.text}" for elem in root if elem.tag and elem.text
    )
