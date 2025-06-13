from datetime import date, datetime
import os
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from typing import Optional, Dict, Any, Union
from dataclasses import dataclass

import pytz
import uvicorn
import anyio
from mcp.server.fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from mindsdb.api.mysql.mysql_proxy.classes.fake_mysql_proxy import FakeMysqlProxy
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE as SQL_RESPONSE_TYPE
from mindsdb.utilities import log
from mindsdb.utilities.config import Config
from mindsdb.interfaces.storage import db

logger = log.getLogger(__name__)


@dataclass
class AppContext:
    db: Any


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """Manage application lifecycle with type-safe context"""
    # Initialize on startup
    db.init()
    try:
        yield AppContext(db=db)
    finally:
        # TODO: We need better way to handle this in storage/db.py
        pass


# Configure server with lifespan
mcp = FastMCP(
    "MindsDB",
    lifespan=app_lifespan,
    dependencies=["mindsdb"]  # Add any additional dependencies
)
# MCP Queries
LISTING_QUERY = "SHOW DATABASES"


@mcp.tool()
def query(query: str, context: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Execute a SQL query against MindsDB

    Args:
        query: The SQL query to execute
        context: Optional context parameters for the query

    Returns:
        Dict containing the query results or error information
    """

    if context is None:
        context = {}

    logger.debug(f'Incoming MCP query: {query}')

    mysql_proxy = FakeMysqlProxy()
    mysql_proxy.set_context(context)

    try:
        result = mysql_proxy.process_query(query)

        if result.type == SQL_RESPONSE_TYPE.OK:
            return {"type": SQL_RESPONSE_TYPE.OK}

        if result.type == SQL_RESPONSE_TYPE.TABLE:
            return {
                "type": SQL_RESPONSE_TYPE.TABLE,
                "data": result.result_set.to_lists(json_types=True),
                "column_names": [
                    column.alias or column.name
                    for column in result.result_set.columns
                ],
            }
        else:
            return {
                "type": SQL_RESPONSE_TYPE.ERROR,
                "error_code": 0,
                "error_message": "Unknown response type"
            }

    except Exception as e:
        logger.error(f"Error processing query: {str(e)}")
        return {
            "type": SQL_RESPONSE_TYPE.ERROR,
            "error_code": 0,
            "error_message": str(e)
        }


@mcp.tool()
def update_ticket_to_new_flight(
          ticket_no: str, new_flight_id: int, passenger_id
)-> str:
     """
     Update the user's ticket to a new valid flight.

     Args:
        ticket_no: The ticket number of the flight 
        new_flight_id: The new flight id
        passenger_id: The id of the passenger
    
     Returns:
        Message confirming the update status of flight
     
     """

     if not passenger_id:
        raise ValueError("No passenger ID given.")

     sql_query_1 = f"""
    SELECT scheduled_departure FROM redshift_datasource.public.flights WHERE flight_id = {new_flight_id}
    """
     new_flight_data = query(sql_query_1)
     if not new_flight_data:
        return "Invalid new flight ID provided."

     try:
        departure_time_str = new_flight_data[0]["scheduled_departure"]
        departure_time = datetime.strptime(departure_time_str, "%Y-%m-%d %H:%M:%S.%f%z")
     except Exception:
        return "Error parsing scheduled departure time from new flight."

     current_time = datetime.now(tz=pytz.timezone("Etc/GMT-3"))
     time_until = (departure_time - current_time).total_seconds()

     if time_until < 3 * 3600:
        return f"Not permitted to reschedule to a flight within 3 hours. Selected flight is at {departure_time}."

     check_ticket_query = f"""
     SELECT ticket_no FROM redshift_datasource.public.tickets WHERE ticket_no = {ticket_no} AND passenger_id = '{passenger_id}'
     """
     ticket_check = query(check_ticket_query)
     if not ticket_check:
        return f"Current signed-in passenger with ID {passenger_id} is not the owner of ticket {ticket_no}."

     update_query = f"""
     UPDATE redshift_datasource.public.ticket_flights SET flight_id = {new_flight_id} WHERE ticket_no = {ticket_no}
     """
     response = query(update_query)
     return "Ticket successfully updated to new flight." if response else "Failed to update ticket."

@mcp.tool()
def cancel_ticket(ticket_no: str, passenger_id) -> str:
     """Cancel the user's ticket and remove it from the db."""
     if not passenger_id:
        raise ValueError("No passenger ID given.")

     check_ticket_query = f"""
     SELECT ticket_no FROM redshift_datasource.public.tickets WHERE ticket_no = {ticket_no} AND passenger_id = '{passenger_id}'
     """
     ticket_check = query(check_ticket_query)
     if not ticket_check:
        return f"Current signed-in passenger with ID {passenger_id} is not the owner of ticket {ticket_no}."

     delete_flight_query = f"""
     DELETE FROM redshift_datasource.public.ticket_flights WHERE ticket_no = {ticket_no}
     """
     delete_resp = query(delete_flight_query)
     return "Ticket successfully cancelled." if delete_resp else "Failed to cancel the ticket."

@mcp.tool()
def search_car_rentals(
    location: Optional[str] = None,
    name: Optional[str] = None,
    price_tier: Optional[str] = None,
    start_date: Optional[Union[datetime, date]] = None,
    end_date: Optional[Union[datetime, date]] = None
) ->list[dict]:
    """
    Search for car rentals based on the given location, name, price_tier, start and end date.
    Args:
        location: The locationn of the car rental. Default to None.
        name: The name of the car rental company. Default to None.
        price_tier: The price_tier of the car rental. Default to None.
        start_date: The start date of the car rental. Default to None.
        end_date: The end date of the car rental. Default to None.
    Returns:
        list[dict]: A list of car rentatl dictionaries matching the search filter.
    """

    conditions = []

    if location: 
        conditions.append(f"location = '{location}'")

    if name: 
        conditions.append(f"name = '{name}'")
    
    if price_tier:
        conditions.append(f"price_tier = '{price_tier}'")
    
    if start_date: 
        conditions.append(f"start_date = '{start_date}'")

    if end_date:
        conditions.append(f"end_date = '{end_date}'")

    where_clause = " AND ".join(conditions)
    
    sql_query = f"SELECT * FROM redshift_datasource.public.car_rentals"
    if where_clause:
        sql_query += f" WHERE {where_clause}"

    return query(sql_query)

@mcp.tool()
def book_car_rental(rental_id: int)->str:
    """
    Book a car rental by its ID.
    Args:
        rental_id: The ID of the car rental to book.
    Returns:
        str: A message indicationg whether the car rental was booked or not.
    """

    sql_query = f"""
    UPDATE redshift_datasource.public.car_rentals SET booked = 1 WHERE id = {rental_id}
    """
    res = query(sql_query)
    return f"Car rental {rental_id} successfully booked." if res else f"No car rental found with ID {rental_id}"


@mcp.tool()
def update_car_rental(
        rental_id: int,
        start_date: Optional[Union[datetime, date]] = None,
        end_date: Optional[Union[datetime, date]] = None
)-> str:
    """
    Update a car rental's start and end dates by its ID.
    Args:
        rental_id: The ID of the car rental to update.
        start_date: The new start date of the car rental. Default to None.
        end_date: The new end date of the car rental. Default to None.
    Returns:
        str: A message indicating whether the car rental was successfully updated or not.
    """
    updates = []
    if start_date:
        updates.append(f"start_date = '{start_date}'")
    if end_date:
        updates.append(f"end_date = '{end_date}'")

    if not updates:
        return "No update parameters provided."

    sql_query = f"""
    UPDATE redshift_datasource.public.car_rentals SET {', '.join(updates)} WHERE id = {rental_id}
    """

    res = query(sql_query)
    return f"Car rental {rental_id} successfully updated." if res else f"No car rental found with ID {rental_id}" 

@mcp.tool()
def cancel_car_rentals(rental_id: int)-> str:
    
    """
    cancel a car rental by its ID.
    Args:
        rental_id: The ID of the car rental to cancel.
    Returns:
        str: A message indicating whether the car rental was successfully cancel or not.
    """

    sql_query = f"""
    UPDATE redshift_datasource.public.car_rentals SET booked = 0 WHERE id {rental_id}
    """
    res = query(sql_query)
    return f"Car rental {rental_id} successfully cancelled." if res else f"No car rental found with ID {rental_id}"


@mcp.tool()
def search_hotels(
    location: Optional[str] = None,
    name: Optional[str] = None,
    price_tier: Optional[str] = None,
    checkin_date: Optional[Union[datetime, date]] = None,
    checkout_date: Optional[Union[datetime, date]] = None,
) -> list[dict]:
    """
    Search for hotels based on location, name, price tier, check-in date, and check-out date.

    Args:
        location (Optional[str]): The location of the hotel. Defaults to None.
        name (Optional[str]): The name of the hotel. Defaults to None.
        price_tier (Optional[str]): The price tier of the hotel. Defaults to None. Examples: Midscale, Upper Midscale, Upscale, Luxury
        checkin_date (Optional[Union[datetime, date]]): The check-in date of the hotel. Defaults to None.
        checkout_date (Optional[Union[datetime, date]]): The check-out date of the hotel. Defaults to None.

    Returns:
        list[dict]: A list of hotel dictionaries matching the search criteria.
    """

    conditions = []

    if location:
        conditions.append(f"location = '{location}'")
    if name:
        conditions.append(f"name = '{name}'")
    if price_tier:
        conditions.append(f"price_tier = '{price_tier}'")
    if checkin_date:
        conditions.append(f"checkin_date = '{checkin_date}'")
    if checkout_date:
        conditions.append(f"checkout_data = '{checkout_date}'")


    where_clause = " AND ".join(conditions)
    
    sql_query = f"SELECT * FROM redshift_datasource.public.hotels"
    if where_clause:
        sql_query += f" WHERE {where_clause}"

    return query(query= sql_query)


@mcp.tool()
def book_hotel(hotel_id: int) -> str:
    """
    Book a hotel by its ID.

    Args:
        hotel_id (int): The ID of the hotel to book.

    Returns:
        str: A message indicating whether the hotel was successfully booked or not.
    """

    sql_query = f"""
    UPDATE redshift_datasource.public.hotels SET booked = 1 WHERE id = {hotel_id}
    """

    res = query(sql_query)
    return "Hotel booked successfully" if res else f"Hotel with id {hotel_id} not found."


@mcp.tool()
def update_hotel(
    hotel_id: int,
    checkin_date: Optional[Union[datetime, date]] = None,
    checkout_date: Optional[Union[datetime, date]] = None,
) -> str:
    """
    Update a hotel's check-in and check-out dates by its ID.

    Args:
        hotel_id (int): The ID of the hotel to update.
        checkin_date (Optional[Union[datetime, date]]): The new check-in date of the hotel. Defaults to None.
        checkout_date (Optional[Union[datetime, date]]): The new check-out date of the hotel. Defaults to None.

    Returns:
        str: A message indicating whether the hotel was successfully updated or not.
    """
    conditions = []

    if checkin_date:
        conditions.append(f"checkin_date = '{checkin_date}'")

    if checkout_date:
        conditions.append(f"checkout_date = '{checkout_date}'")

    if not conditions:
        return "No update parameters provided."

    sql_query = f"""
    UPDATE redshift_datasource.public.hotels SET {', '.join(conditions)} WHERE id = {hotel_id}
    """
    res = query(sql_query)
    return f"Hotel {hotel_id} successfully booked." if res else f"No hotel found with ID {hotel_id}"


@mcp.tool()
def cancel_hotel(hotel_id: int) -> str:
    """
    Cancel a hotel by its ID.

    Args:
        hotel_id (int): The ID of the hotel to cancel.

    Returns:
        str: A message indicating whether the hotel was successfully cancelled or not.
    """
    
    sql_query = f"""
    UPDATE redshift_datasource.public.hotels SET booked = 0 WHERE id = {hotel_id}
    """

    res = query(sql_query)
    return f"Hotel with id {hotel_id} cancelled successfully"if res else f"No hotel found with id {hotel_id}"


@mcp.tool()
def search_trip_recommendations(
    location: Optional[str] = None,
    name: Optional[str] = None,
    keywords: Optional[str] = None,
) -> list[dict]:
    """
    Search for trip recommendations based on location, name, and keywords.

    Args:
        location (Optional[str]): The location of the trip recommendation. Defaults to None.
        name (Optional[str]): The name of the trip recommendation. Defaults to None.

    Returns:
        list[dict]: A list of trip recommendation dictionaries matching the search criteria.
    """

    conditions = [] 

    if location:
        conditions.append(f"location = '{location}'")

    if name:
        conditions.append(f"name = '{name}'")
    if keywords:
        conditions.append(f"keywords = '{keywords}'")

    where_clause = " AND ".join(conditions)
    sql_query = f"SELECT * FROM redshift_datasource.public.trip_recommendations"
    if where_clause:
        sql_query += f" WHERE {where_clause}"

    return query(sql_query)
    

@mcp.tool()
def book_excursion(recommendation_id: int) -> str:
    """
    Book a excursion by its recommendation ID.

    Args:
        recommendation_id (int): The ID of the trip recommendation to book.

    Returns:
        str: A message indicating whether the trip recommendation was successfully booked or not.
    """

    sql_query = f"""
    UPDATE redshift_datasource.public.trip_recommendations SET booked = 1 WHERE id = {recommendation_id}
    """

    res = query(sql_query)
    return "Excursion booked successfully" if res else f"Excursion not found with id {recommendation_id}"
    


@mcp.tool()
def cancel_excursion(recommendation_id: int) -> str:
    """
    Cancel a trip recommendation by its ID.

    Args:
        recommendation_id (int): The ID of the trip recommendation to cancel.

    Returns:
        str: A message indicating whether the trip recommendation was successfully cancelled or not.
    """

    sql_query = f"""
    UPDATE redshift_datasource.public.trip_recommendations SET booked = 0 WHERE id = {recommendation_id}
    """

    res = query(sql_query)
    return "Excusrsion cacelled successfully" if res else f"Excursion not found with id {recommendation_id}"
      
@mcp.tool()
def list_databases() -> Dict[str, Any]:
    """
    List all databases in MindsDB along with their tables

    Returns:
        Dict containing the list of databases and their tables
    """

    mysql_proxy = FakeMysqlProxy()

    try:
        result = mysql_proxy.process_query(LISTING_QUERY)
        if result.type == SQL_RESPONSE_TYPE.ERROR:
            return {
                "type": "error",
                "error_code": result.error_code,
                "error_message": result.error_message,
            }

        elif result.type == SQL_RESPONSE_TYPE.OK:
            return {"type": "ok"}

        elif result.type == SQL_RESPONSE_TYPE.TABLE:
            data = result.result_set.to_lists(json_types=True)
            return data

    except Exception as e:
        return {
            "type": "error",
            "error_code": 0,
            "error_message": str(e),
        }


class CustomAuthMiddleware(BaseHTTPMiddleware):
    """Custom middleware to handle authentication basing on header 'Authorization'
    """
    async def dispatch(self, request: Request, call_next):
        mcp_access_token = os.environ.get('MINDSDB_MCP_ACCESS_TOKEN')
        if mcp_access_token is not None:
            auth_token = request.headers.get('Authorization', '').partition('Bearer ')[-1]
            if mcp_access_token != auth_token:
                return Response(status_code=401, content="Unauthorized", media_type="text/plain")

        response = await call_next(request)

        return response


async def run_sse_async() -> None:
    """Run the server using SSE transport."""
    starlette_app = mcp.sse_app()
    starlette_app.add_middleware(CustomAuthMiddleware)

    config = uvicorn.Config(
        starlette_app,
        host=mcp.settings.host,
        port=mcp.settings.port,
        log_level=mcp.settings.log_level.lower(),
    )
    server = uvicorn.Server(config)
    await server.serve()


def start(*args, **kwargs):
    """Start the MCP server
    Args:
        host (str): Host to bind to
        port (int): Port to listen on
    """
    config = Config()
    port = int(config['api'].get('mcp', {}).get('port', 47337))
    host = config['api'].get('mcp', {}).get('host', '127.0.0.1')

    logger.info(f"Starting MCP server on {host}:{port}")
    mcp.settings.host = host
    mcp.settings.port = port

    try:
        anyio.run(run_sse_async)
    except Exception as e:
        logger.error(f"Error starting MCP server: {str(e)}")
        raise


if __name__ == "__main__":
    start()
