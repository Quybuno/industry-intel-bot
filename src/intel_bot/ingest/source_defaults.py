"""Hardcoded default extract sources."""
from __future__ import annotations

import re
import unicodedata
from typing import Any

RSS_SOURCES = [
    {'url': 'https://venturebeat.com/category/ai/feed/', 'name': 'VentureBeat AI', 'domain': 'ai_tech'},
    {'url': 'https://the-decoder.com/feed/', 'name': 'The Decoder', 'domain': 'ai_tech'},
    {'url': 'https://www.technologyreview.com/feed/', 'name': 'MIT Technology Review', 'domain': 'ai_tech'},
    {'url': 'https://feeds.arstechnica.com/arstechnica/technology-lab', 'name': 'Ars Technica Tech', 'domain': 'ai_tech'},
    {'url': 'https://www.theverge.com/rss/ai-artificial-intelligence/index.xml', 'name': 'The Verge AI', 'domain': 'ai_tech'},
    {'url': 'https://techcrunch.com/category/artificial-intelligence/feed/', 'name': 'TechCrunch AI', 'domain': 'ai_tech'},
    {'url': 'https://www.constructiondive.com/feeds/news/', 'name': 'Construction Dive', 'domain': 'construction'},
    {'url': 'https://www.enr.com/rss/all', 'name': 'ENR', 'domain': 'construction'},
    {'url': 'https://www.bdcnetwork.com/rss.xml', 'name': 'Building Design+Const.', 'domain': 'construction'},
    {'url': 'https://www.autodesk.com/blogs/construction/feed/', 'name': 'Autodesk Construction', 'domain': 'construction'},
    {'url': 'https://www.industryweek.com/rss/all', 'name': 'Industry Week', 'domain': 'manufacturing'},
    {'url': 'https://www.manufacturingnews.com/rss/', 'name': 'Manufacturing News', 'domain': 'manufacturing'},
    {'url': 'https://www.assemblymag.com/rss/all', 'name': 'Assembly Magazine', 'domain': 'manufacturing'},
    {'url': 'https://www.sme.org/rss/', 'name': 'SME', 'domain': 'manufacturing'},
    {'url': 'https://www.achrnews.com/rss/news', 'name': 'ACHR News', 'domain': 'hvac_mep'},
    {'url': 'https://www.contractingbusiness.com/rss/all', 'name': 'Contracting Business', 'domain': 'hvac_mep'},
    {'url': 'https://www.hpacmag.com/feed/', 'name': 'HPAC Magazine', 'domain': 'hvac_mep'},
    {'url': 'https://www.ashrae.org/news/ashraejournal/rss', 'name': 'ASHRAE Journal', 'domain': 'hvac_mep'},
    {'url': 'https://ictnews.vn/rss/home.rss', 'name': 'ICT News VN', 'domain': 'ai_tech'},
    {'url': 'https://vneconomy.vn/rss/cong-nghe.rss', 'name': 'VnEconomy Công nghệ', 'domain': 'ai_tech'},
]

DOMAIN_INDUSTRIES = {
    'ai_tech': ['ai', 'tech'],
    'construction': ['construction'],
    'manufacturing': ['manufacturing'],
    'hvac_mep': ['hvac', 'mep'],
}


def _slugify(value: str) -> str:
    ascii_value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    slug = re.sub(r'[^a-z0-9]+', '_', ascii_value.lower()).strip('_')
    return slug or 'source'


def default_rss_sources() -> list[dict[str, Any]]:
    """Return hardcoded RSS sources in the repository source schema."""
    sources: list[dict[str, Any]] = []
    for source in RSS_SOURCES:
        sources.append({
            'id': _slugify(source['name']),
            'name': source['name'],
            'type': 'rss',
            'url_or_query': source['url'],
            'enabled': True,
            'tier': 6,
            'domain': source['domain'],
            'industries': DOMAIN_INDUSTRIES.get(source['domain'], [source['domain']]),
        })
    return sources
