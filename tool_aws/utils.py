from pyproj import Proj, transform


def reprojectBBox(bbox, srid_to, srid_from=2056):
    if srid_to == srid_from:
        return bbox
    srid_in = Proj('+init=EPSG:%s' % srid_from)
    srid_out = Proj('+init=EPSG:%s' % srid_to)
    p_left = transform(srid_in, srid_out, bbox[0], bbox[1])
    p_right = transform(srid_in, srid_out, bbox[2], bbox[3])
    return p_left + p_right
