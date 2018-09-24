<?php


namespace Verse\Storage\Spec;


class Compare
{
    const EQ              = '=';
    const NOT_EQ          = '!=';
    const EMPTY_OR_EQ     = '?=';
    const EMPTY_OR_NOT_EQ = '?!=';
    const GRATER          = '>';
    const LESS            = '<';
    const GRATER_OR_EQ    = '>=';
    const LESS_OR_EQ      = '<=';
    const IN              = 'in';
    const ANY             = 'any';
    const STR_BEGINS      = 'ILIKE%';
    const STR_ENDS        = '%ILIKE';
    const SUB_STR         = '%ILIKE%';
}