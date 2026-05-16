use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery};
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use datafusion::datasource::file_format::options::ParquetReadOptions;
use datafusion::execution::context::SessionContext;
use futures_util::StreamExt;
use futures_util::stream::TryStreamExt;
use prost::Message;
use tonic::{Request, Response, Status};

struct FlightSqlServer {
    ctx: SessionContext,
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServer {
    type FlightService = Self;

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = query.query;
        let df = self
            .ctx
            .sql(&sql)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        let ticket = TicketStatementQuery {
            statement_handle: sql.into_bytes().into(),
        };

        let endpoint =
            FlightEndpoint::new().with_ticket(Ticket::new(ticket.as_any().encode_to_vec()));

        let flight_info = FlightInfo::new()
            .try_with_schema(df.schema().as_arrow())
            .map_err(|error| Status::internal(error.to_string()))?
            .with_endpoint(endpoint)
            .with_descriptor(request.into_inner());

        Ok(Response::new(flight_info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let sql = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|error| Status::internal(error.to_string()))?;

        let df = self
            .ctx
            .sql(&sql)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;

        let stream = df
            .execute_stream()
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(|error| FlightError::ExternalError(Box::new(error)));

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .build(stream)
            .map_err(|error| Status::internal(error.to_string()))
            .map_err(|error| Status::internal(error.to_string()))
            .boxed();

        Ok(Response::new(flight_data_stream))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "test_parquet",
        "data/fhvhv_tripdata_2026-02.parquet",
        ParquetReadOptions::new(),
    )
    .await?;

    let addr = "127.0.0.1:50051".parse()?;
    let service = FlightServiceServer::new(FlightSqlServer {ctx});
    
    tonic::transport::Server::builder().add_service(service).serve(addr).await?;

    Ok(())
}
