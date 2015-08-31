/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package varys.framework.slave.ui

import java.util.Date
import javax.servlet.http.HttpServletRequest

import akka.pattern.ask
import net.liftweb.json.JsonAST.JValue
import varys.framework._
import varys.ui.UIUtils

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.xml.Node

private[varys] class IndexPage(parent: SlaveWebUI) {
  val slaveActor = parent.slave.self
  val slave = parent.slave
  val timeout = parent.timeout

  def renderJson(request: HttpServletRequest): JValue = {
    val stateFuture = (slaveActor ? RequestSlaveState)(timeout).mapTo[SlaveState]
    val slaveState = Await.result(stateFuture, 30.seconds)
    JsonProtocol.writeSlaveState(slaveState)
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val stateFuture = (slaveActor ? RequestSlaveState)(timeout).mapTo[SlaveState]
    val slaveState = Await.result(stateFuture, 30.seconds)

    val flowHeaders = Seq("Start Time", "Source IP", "Source Port", "Destination IP", "Destination Port")
    val flowTable = UIUtils.listingTable(flowHeaders, flowRow, slaveState.runningFlows)

    val content =
        <div class="row-fluid"> <!-- Slave Details -->
          <div class="span12">
            <ul class="unstyled">
              <li><strong>ID:</strong> {slaveState.slaveId}</li>
              <li><strong>
                Master URL:</strong> {slaveState.masterUrl}
              </li>
              <li><strong>RxBps:</strong> {slaveState.rxBps}</li>
              <li><strong>TxBps:</strong> {slaveState.txBps}</li>
            </ul>
            <p><a href={slaveState.masterWebUiUrl}>Back to Master</a></p>
          </div>
        </div>

        <div class="row-fluid">
          <div class="span12">
            <h4> Running Flows </h4>
            {flowTable}
          </div>
        </div>;

    UIUtils.basicVarysPage(content, "Varys Slave at %s:%s".format(
      slaveState.host, slaveState.port))
  }

  def flowRow(flow: Flow): Seq[Node] = {
    <tr>
      <td>{FrameworkWebUI.formatDate(new Date(flow.startTime))}</td>
      <td>{flow.sIP}</td>
      <td>{flow.sPort}</td>
      <td>{flow.dIP}</td>
      <td>{flow.dPort}</td>
    </tr>
  }
}
