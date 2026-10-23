import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import fetch_data as f


class CountySourceTests(unittest.TestCase):
    def test_archive_pagination_and_latest_snapshot(self):
        pages = [b'''<EnumerationResults><Blobs><Blob><Name>reports/incidentsummary/IncidentSummary_20261014.csv</Name></Blob></Blobs><NextMarker>next page</NextMarker></EnumerationResults>''', b'''<EnumerationResults><Blobs><Blob><Name>reports/incidentsummary/IncidentSummary.csv</Name><Properties><Last-Modified>Wed, 21 Oct 2026 11:25:01 GMT</Last-Modified></Properties></Blob><Blob><Name>reports/incidentsummary/PlaceHolder.txt</Name></Blob></Blobs></EnumerationResults>''']
        with patch.object(f.urllib.request, 'urlopen', side_effect=[io.BytesIO(p) for p in pages]) as request:
            files = f.list_remote_files()
        self.assertEqual(list(files), ['IncidentSummary_20261014.csv', 'IncidentSummary_20261021.csv'])
        self.assertTrue(files['IncidentSummary_20261021.csv'].endswith('/IncidentSummary.csv'))
        self.assertIn('marker=next+page', request.call_args_list[1].args[0].full_url)

    def test_empty_listing_fails(self):
        with patch.object(f.urllib.request, 'urlopen', return_value=io.BytesIO(b'<EnumerationResults><Blobs/></EnumerationResults>')):
            with self.assertRaises(ValueError):
                f.list_remote_files()

    def test_error_page_never_enters_cache(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(f, 'CACHE_DIR', Path(folder)):
            with patch.object(f.urllib.request, 'urlopen', return_value=io.BytesIO(b'<html>Error</html>')):
                with self.assertRaises(ValueError):
                    f.download('IncidentSummary_20261021.csv', 'https://example.test/report')
            self.assertEqual(list(Path(folder).iterdir()), [])

    def test_valid_csv_cached_and_reused(self):
        payload = b'CreatedOn1,EventDescription,CaseNum\n10/18/2026 1:00:00 PM,THEFT,123\n'
        with tempfile.TemporaryDirectory() as folder, patch.object(f, 'CACHE_DIR', Path(folder)):
            with patch.object(f.urllib.request, 'urlopen', return_value=io.BytesIO(payload)) as request:
                dest, new = f.download('IncidentSummary_20261021.csv', 'https://example.test/report')
                self.assertTrue(new)
                self.assertEqual(dest.read_bytes(), payload)
                self.assertFalse(f.download(dest.name, 'https://example.test/report')[1])
                self.assertEqual(request.call_count, 1)


if __name__ == '__main__':
    unittest.main()
