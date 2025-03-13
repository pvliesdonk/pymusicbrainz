from functools import cached_property

import mbdata.models
import sqlalchemy as sa

from pymusicbrainz import WorkID, get_db_session, ReleaseGroup, Medium, Track
from pymusicbrainz.dataclasses_old import Recording, escape, Release
from pymusicbrainz.datatypes import PerformanceWorkAttributes
from pymusicbrainz.exceptions import MBIDNotExistsError


class Work(MusicBrainzObject):
    def __init__(self,
                 in_obj: WorkID | mbdata.models.Work | str) -> None:
        with get_db_session() as session:
            if isinstance(in_obj, mbdata.models.Work):
                w: mbdata.models.Work = session.merge(in_obj)
            else:
                if isinstance(in_obj, str):
                    in_obj = WorkID(in_obj)
                stmt = sa.select(mbdata.models.Work).where(mbdata.models.Work.gid == str(in_obj))
                w: mbdata.models.Work = session.scalar(stmt)

                if w is None:
                    raise MBIDNotExistsError(f"No Work with ID '{str(in_obj)}'")

            self.id: WorkID = WorkID(str(w.gid))
            self._db_id: int = w.id
            self.title: str = w.name
            self.disambiguation: str = w.comment
            self.type: str = w.type.name if w.type is not None else None

    @cached_property
    def performances(self) -> dict[PerformanceWorkAttributes, list[Recording]]:
        results = {PerformanceWorkAttributes.ALL: [], PerformanceWorkAttributes.NONE: []}
        from .object_cache import get_recording
        with get_db_session() as session:

            stmt = (

                sa.select(mbdata.models.Recording, mbdata.models.LinkAttribute)
                .select_from(
                    sa.join(
                        sa.join(mbdata.models.LinkRecordingWork, mbdata.models.Recording),
                        sa.join(mbdata.models.LinkAttribute, mbdata.models.Link),
                        isouter=True
                    )
                )
                .where(mbdata.models.LinkRecordingWork.entity1_id == str(self._db_id))
            )

            res = session.execute(stmt)

            for (r, la) in res:
                rec: Recording = get_recording(r)
                if rec not in results[PerformanceWorkAttributes.ALL]:
                    results[PerformanceWorkAttributes.ALL].append(rec)

                if la is None:
                    results[PerformanceWorkAttributes.NONE].append(rec)
                else:
                    att = PerformanceWorkAttributes(la.attribute_type.name)
                    if att in results.keys():
                        results[att].append(rec)
                    else:
                        results[att] = [rec]

        return results

    def performance_by_type(self, types: list[PerformanceWorkAttributes]) -> list[Recording]:
        results = None
        for t in types:
            if t in self.performances.keys():
                if results is None:
                    results = self.performances[t]
                else:
                    results = [r for r in results if r in self.performances[t]]
                    results = list(set(results))
        if results is None:
            return []
        return results

    def __str__(self):
        return f"{self.title}  [{self.id}]"

    def __rich__(self):
        return f"{escape(self.title)}  \[[link={self.url}]{self.id}[/link]\]"

    def __eq__(self, other):
        if isinstance(other, Work):
            return self.id == other.id
        else:
            return False

    @cached_property
    def url(self) -> str:
        return f"https://musicbrainz.org/work/{self.id}"

    def __hash__(self):
        return hash(self.id)

    def __contains__(self, item):
        if isinstance(item, Artist):
            raise NotImplementedError
        if isinstance(item, ReleaseGroup):
            raise NotImplementedError
        if isinstance(item, Release):
            raise NotImplementedError
        if isinstance(item, Medium):
            raise NotImplementedError
        if isinstance(item, Track):
            return item.recording in self.performances['all']
        if isinstance(item, Recording):
            return item in self.performances['all']
