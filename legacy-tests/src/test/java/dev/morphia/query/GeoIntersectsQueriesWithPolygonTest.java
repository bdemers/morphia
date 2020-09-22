package dev.morphia.query;

import com.mongodb.client.model.geojson.GeometryCollection;
import com.mongodb.client.model.geojson.LineString;
import com.mongodb.client.model.geojson.MultiPoint;
import com.mongodb.client.model.geojson.MultiPolygon;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Polygon;
import com.mongodb.client.model.geojson.PolygonCoordinates;
import com.mongodb.client.model.geojson.Position;
import dev.morphia.TestBase;
import dev.morphia.geo.model.AllTheThings;
import dev.morphia.geo.model.Area;
import dev.morphia.geo.model.City;
import dev.morphia.geo.model.Regions;
import dev.morphia.geo.model.Route;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static dev.morphia.query.experimental.filters.Filters.geoIntersects;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class GeoIntersectsQueriesWithPolygonTest extends TestBase {
    @Test
    public void shouldFindAPointThatLiesInAQueryPolygon() {
        // given
        City manchester = new City("Manchester", new Point(new Position(53.4722454, -2.2235922)));
        getDs().save(manchester);
        City london = new City("London", new Point(new Position(51.5286416, -0.1015987)));
        getDs().save(london);
        City sevilla = new City("Sevilla", new Point(new Position(37.4057731, -5.966287)));
        getDs().save(sevilla);

        getDs().ensureIndexes();

        // when
        List<City> matchingCity = getDs().find(City.class)
                                         .filter(geoIntersects("location", new Polygon(asList(
                                             new Position(37.40759155713022, -5.964911067858338),
                                             new Position(37.40341208875179, -5.9643941558897495),
                                             new Position(37.40297396667302, -5.970452763140202),
                                             new Position(37.40759155713022, -5.964911067858338))))).iterator().toList();

        // then
        assertThat(matchingCity.size(), is(1));
        assertThat(matchingCity.get(0), is(sevilla));
    }

    @Test
    public void shouldFindAreasThatAPolygonIntersects() {
        // given
        Area sevilla = new Area("Spain",
            new Polygon(asList(new Position(37.40759155713022, -5.964911067858338),
                new Position(37.40341208875179, -5.9643941558897495),
                new Position(37.40297396667302, -5.970452763140202),
                new Position(37.40759155713022, -5.964911067858338))));
        getDs().save(sevilla);

        Area newYork = new Area("New York",
            new Polygon(asList(new Position(40.75981395319104, -73.98302106186748),
                new Position(40.7636824529618, -73.98049869574606),
                new Position(40.76962974853814, -73.97964206524193),
                new Position(40.75981395319104, -73.98302106186748))));
        getDs().save(newYork);
        Area london = new Area("London",
            new Polygon(asList(new Position(51.507780365645885, -0.21786745637655258),
                new Position(51.50802478194237, -0.21474729292094707),
                new Position(51.5086863655597, -0.20895397290587425),
                new Position(51.507780365645885, -0.21786745637655258))));
        getDs().save(london);
        getDs().ensureIndexes();

        // when
        List<Position> es = asList(
            new Position(37.4056048, -5.9666089),
            new Position(37.404497, -5.9640557),
            new Position(37.407239, -5.962988),
            new Position(37.4056048, -5.9666089));
        List<Area> areaContainingPoint = getDs().find(Area.class)
                                                .filter(geoIntersects("area", new Polygon(es))).iterator().toList();

        // then
        assertThat(areaContainingPoint.size(), is(1));
        assertThat(areaContainingPoint.get(0), is(sevilla));
    }

    @Test
    @Ignore("counts are off.  revisit later")
    public void shouldFindGeometryCollectionsWhereTheGivenPointIntersectsWithOneOfTheEntities() {
        // given
        AllTheThings sevilla = new AllTheThings("Spain", new GeometryCollection(asList(
            new MultiPoint(asList(
                new Position(37.40759155713022, -5.964911067858338),
                new Position(37.40341208875179, -5.9643941558897495),
                new Position(37.40297396667302, -5.970452763140202),
                new Position(37.40759155713022, -5.964911067858338),
                new Position(37.40341208875179, -5.9643941558897495),
                new Position(37.40297396667302, -5.970452763140202),
                new Position(37.40759155713022, -5.964911067858338),
                new Position(37.38744598813355, -6.001141928136349),
                new Position(37.385990973562, -6.002588979899883),
                new Position(37.386126928031445, -6.002463921904564),
                new Position(37.38744598813355, -6.001141928136349))))));
        getDs().save(sevilla);

        // insert something that's not a geocollection
        Regions usa = new Regions("US", new MultiPolygon(asList(
            new PolygonCoordinates(asList(new Position(40.75981395319104, -73.98302106186748),
                new Position(40.7636824529618, -73.98049869574606),
                new Position(40.76962974853814, -73.97964206524193),
                new Position(40.75981395319104, -73.98302106186748))),
            new PolygonCoordinates(asList(new Position(28.326568258926272, -81.60542246885598),
                new Position(28.327541397884488, -81.6022228449583),
                new Position(28.32950334995985, -81.60564735531807),
                new Position(28.326568258926272, -81.60542246885598))))));
        getDs().save(usa);

        AllTheThings london = new AllTheThings("London", new GeometryCollection(asList(
            new LineString(asList(
                new Position(51.507780365645885, -0.21786745637655258),
                new Position(51.50802478194237, -0.21474729292094707),
                new Position(51.5086863655597, -0.20895397290587425))),
            new Polygon(asList(
                new Position(51.498216362670064, 0.0074849557131528854),
                new Position(51.49176875129342, 0.01821178011596203),
                new Position(51.492886897176504, 0.05523204803466797),
                new Position(51.49393044412136, 0.06663135252892971),
                new Position(51.498216362670064, 0.0074849557131528854))))));
        getDs().save(london);
        getDs().ensureIndexes();

        // when
        List<AllTheThings> everythingInTheUK = getDs().find(AllTheThings.class)
                                                      .filter(geoIntersects("everything", new Polygon(asList(
                                                          new Position(37.4056048, -5.9666089),
                                                          new Position(37.404497, -5.9640557),
                                                          new Position(37.407239, -5.962988),
                                                          new Position(37.4056048, -5.9666089))))).iterator().toList();

        // then
        assertThat(everythingInTheUK.size(), is(1));
        assertThat(everythingInTheUK.get(0), is(sevilla));
    }

    @Test
    public void shouldFindRegionsThatAPolygonCrosses() {
        // given
        Regions sevilla = new Regions("Spain", new MultiPolygon(asList(
            new PolygonCoordinates(asList(
                new Position(37.40759155713022, -5.964911067858338),
                new Position(37.40341208875179, -5.9643941558897495),
                new Position(37.40297396667302, -5.970452763140202),
                new Position(37.40759155713022, -5.964911067858338))),
            new PolygonCoordinates(asList(
                new Position(37.38744598813355, -6.001141928136349),
                new Position(37.385990973562, -6.002588979899883),
                new Position(37.386126928031445, -6.002463921904564),
                new Position(37.38744598813355, -6.001141928136349))))));
        getDs().save(sevilla);

        Regions usa = new Regions("US", new MultiPolygon(asList(
            new PolygonCoordinates(asList(
                new Position(40.75981395319104, -73.98302106186748),
                new Position(40.7636824529618, -73.98049869574606),
                new Position(40.76962974853814, -73.97964206524193),
                new Position(40.75981395319104, -73.98302106186748))),
            new PolygonCoordinates(asList(
                new Position(28.326568258926272, -81.60542246885598),
                new Position(28.327541397884488, -81.6022228449583),
                new Position(28.32950334995985, -81.60564735531807),
                new Position(28.326568258926272, -81.60542246885598))))));
        getDs().save(usa);

        Regions london = new Regions("London", new MultiPolygon(asList(
            new PolygonCoordinates(asList(
                new Position(51.507780365645885, -0.21786745637655258),
                new Position(51.50802478194237, -0.21474729292094707),
                new Position(51.5086863655597, -0.20895397290587425),
                new Position(51.507780365645885, -0.21786745637655258))),
            new PolygonCoordinates(asList(
                new Position(51.498216362670064, 0.0074849557131528854),
                new Position(51.49176875129342, 0.01821178011596203),
                new Position(51.492886897176504, 0.05523204803466797),
                new Position(51.49393044412136, 0.06663135252892971),
                new Position(51.498216362670064, 0.0074849557131528854))))));
        getDs().save(london);
        getDs().ensureIndexes();

        // when
        List<Regions> regionsInTheUK = getDs().find(Regions.class)
                                              .filter(geoIntersects("regions", new Polygon(asList(
                                                  new Position(37.4056048, -5.9666089),
                                                  new Position(37.404497, -5.9640557),
                                                  new Position(37.407239, -5.962988),
                                                  new Position(37.4056048, -5.9666089))))).iterator()
                                              .toList();

        // then
        assertThat(regionsInTheUK.size(), is(1));
        assertThat(regionsInTheUK.get(0), is(sevilla));
    }

    @Test
    public void shouldFindRoutesThatCrossAQueryPolygon() {
        // given
        Route sevilla = new Route("Spain", new LineString(asList(
            new Position(37.4056048, -5.9666089),
            new Position(37.404497, -5.9640557))));
        getDs().save(sevilla);

        Route newYork = new Route("New York", new LineString(asList(
            new Position(40.75981395319104, -73.98302106186748),
            new Position(40.7636824529618, -73.98049869574606),
            new Position(40.76962974853814, -73.97964206524193))));
        getDs().save(newYork);
        Route london = new Route("London", new LineString(asList(
            new Position(51.507780365645885, -0.21786745637655258),
            new Position(51.50802478194237, -0.21474729292094707),
            new Position(51.5086863655597, -0.20895397290587425))));
        getDs().save(london);
        Route londonToParis = new Route("London To Paris", new LineString(asList(
            new Position(51.5286416, -0.1015987),
            new Position(48.858859, 2.3470599))));
        getDs().save(londonToParis);
        getDs().ensureIndexes();

        // when
        List<Route> routeContainingPoint = getDs().find(Route.class)
                                                  .filter(geoIntersects("route", new Polygon(asList(
                                                      new Position(37.40759155713022, -5.964911067858338),
                                                      new Position(37.40341208875179, -5.9643941558897495),
                                                      new Position(37.40297396667302, -5.970452763140202),
                                                      new Position(37.40759155713022, -5.964911067858338))))).iterator()
                                                  .toList();

        // then
        assertThat(routeContainingPoint.size(), is(1));
        assertThat(routeContainingPoint.get(0), is(sevilla));
    }

}
