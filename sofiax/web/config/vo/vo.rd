<resource schema="wallaby">
   <meta name="title">WALLABY - the ASKAP HI All-Sky Survey</meta>

   <table id="run" onDisk="True" adql="True">
      <column name="id" type="bigint" unit="" ucd="meta.id;meta.main" required="True"/>
      <column name="name" type="text" unit="" ucd="meta.id"/>
   </table>

   <table id="instance" onDisk="True" adql="True">
      <column name="id" type="bigint" unit="" ucd="meta.id;meta.main" required="True"/>
      <column name="run_id" type="bigint" unit="" ucd="meta.id" required="True"/>
      <column name="filename" type="text" unit="" ucd="meta.id"/>
      <column name="run_date" type="timestamp" unit="" ucd="meta.id"/>
      <column name="version" type="text" unit="" ucd="meta.id"/>
      <foreignKey source="run_id" dest="id" inTable="run"/>
   </table>

   <table id="detection" onDisk="True" adql="True">
      <index columns="id"/>

      <meta name="_associatedDatalinkService">
         <meta name="serviceId">dl</meta>
         <meta name="idColumn">id</meta>
      </meta>

      <column name="id" type="bigint" unit="" ucd="meta.id;meta.main" required="True" verbLevel="1"/>
      <column name="name" type="text" unit="" ucd="meta.id"/>
      <column name="run_id" type="bigint" unit="" ucd="meta.id" required="True"/>
      <column name="instance_id" type="bigint" unit="" ucd="meta.id" required="True"/>
      <column type="double precision" name="x" unit="pix" ucd="pos.cartesian.x"/>
      <column type="double precision" name="y" unit="pix" ucd="pos.cartesian.y"/>
      <column type="double precision" name="z" unit="pix" ucd="pos.cartesian.z"/>
      <column type="double precision" name="x_min" unit="pix" ucd="pos.cartesian.x;stat.min"/>
      <column type="double precision" name="x_max" unit="pix" ucd="pos.cartesian.x;stat.max"/>
      <column type="double precision" name="y_min" unit="pix" ucd="pos.cartesian.y;stat.min"/>
      <column type="double precision" name="y_max" unit="pix" ucd="pos.cartesian.y;stat.max"/>
      <column type="double precision" name="z_min" unit="pix" ucd="pos.cartesian.z;stat.min"/>
      <column type="double precision" name="z_max" unit="pix" ucd="pos.cartesian.z;stat.max"/>
      <column type="double precision" name="n_pix" unit="" ucd="meta.number;instr.pixel"/>
      <column type="double precision" name="f_min" unit="Jy/beam" ucd="phot.flux.density;stat.min"/>
      <column type="double precision" name="f_max" unit="Jy/beam" ucd="phot.flux.density;stat.max"/>
      <column type="double precision" name="f_sum" unit="Jy/beam" ucd="phot.flux"/>
      <column type="double precision" name="rel" unit="" ucd="stat.probability"/>
      <column type="integer" name="flag" unit="" ucd="meta.code.qual" required="True"/>
      <column type="double precision" name="rms" unit="Jy/beam" ucd="instr.det.noise"/>
      <column type="double precision" name="w20" unit="Hz" ucd="spect.line.width"/>
      <column type="double precision" name="w50" unit="Hz" ucd="spect.line.width"/>
      <column type="double precision" name="ell_maj" unit="pix" ucd="phys.angSize"/>
      <column type="double precision" name="ell_min" unit="pix" ucd="phys.angSize"/>
      <column type="double precision" name="ell_pa" unit="deg" ucd="pos.posAng"/>
      <column type="double precision" name="ell3s_maj" unit="pix" ucd="phys.angSize"/>
      <column type="double precision" name="ell3s_min" unit="pix" ucd="phys.angSize"/>
      <column type="double precision" name="ell3s_pa" unit="deg" ucd="pos.posAng"/>
      <column type="double precision" name="kin_pa" unit="deg" ucd="pos.posAng"/>
      <column type="double precision" name="err_x" unit="pix" ucd="stat.error;pos.cartesian.x"/>
      <column type="double precision" name="err_y" unit="pix" ucd="stat.error;pos.cartesian.y"/>
      <column type="double precision" name="err_z" unit="pix" ucd="stat.error;pos.cartesian.z"/>
      <column type="double precision" name="err_f_sum" unit="Jy/beam" ucd="stat.error;phot.flux"/>
      <column type="double precision" name="ra" unit="deg" ucd="pos.eq.ra;meta.main" verbLevel="1"/>
      <column type="double precision" name="dec" unit="deg" ucd="pos.eq.dec;meta.main" verbLevel="1"/>
      <column type="double precision" name="freq" unit="Hz" ucd="em.freq"/>
      <column type="double precision" name="l" unit="deg" ucd="pos.galactic.lon"/>
      <column type="double precision" name="b" unit="deg" ucd="pos.galactic.lat"/>
      <column type="double precision" name="v_rad" unit="m/s" ucd="spect.dopplerVeloc.radio"/>
      <column type="double precision" name="v_opt" unit="m/s" ucd="spect.dopplerVeloc.opt"/>
      <column type="double precision" name="v_app" unit="m/s" ucd="spect.dopplerVeloc"/>
      <foreignKey source="run_id" dest="id" inTable="run"/>
      <foreignKey source="instance_id" dest="id" inTable="instance"/>

   </table>

	<service id="dl" allowed="dlget,dlmeta">
		<meta name="title">Wallaby Detections Datalink</meta>
        <meta name="dlget.description">Wallaby Detections Datalink</meta>
		<datalinkCore>
           <descriptorGenerator>
            <setup>
              <code>
                 class CustomDescriptor(ProductDescriptor):
                     def __init__(self, id):
                        super(ProductDescriptor, self).__init__()
                        self.pubDID = id
                        self.mime = ""
                        self.accref = ""
                        self.accessPath = ""
                        self.access_url = ""
                        self.suppressAutoLinks = True
                 </code>
             </setup>
            <code>
               return CustomDescriptor(pubDID)
            </code>
          </descriptorGenerator>

           <metaMaker>
              <code>
                  import os
                  from urllib.parse import urlencode

                  server_url = os.environ.get('PRODUCT_URL', "http://localhost:8080")

                  params = {"id": descriptor.pubDID, "product": "cube"}
                  url = "{1}/sofiax_detections/detection_products?{0}".format(urlencode(params), server_url)
                  yield LinkDef(descriptor.pubDID, url, contentType="image/fits", description="SoFiA-2 Detection Cube", semantics="#preview")

                  params = {"id": descriptor.pubDID, "product": "moment0"}
                  url = "{1}/sofiax_detections/detection_products?{0}".format(urlencode(params), server_url)
                  yield LinkDef(descriptor.pubDID, url, contentType="image/fits", description="SoFiA-2 Detection Moment0", semantics="#preview")

                  params = {"id": descriptor.pubDID, "product": "moment1"}
                  url = "{1}/sofiax_detections/detection_products?{0}".format(urlencode(params), server_url)
                  yield LinkDef(descriptor.pubDID, url, contentType="image/fits", description="SoFiA-2 Detection Moment1", semantics="#preview")

                  params = {"id": descriptor.pubDID, "product": "moment2"}
                  url = "{1}/sofiax_detections/detection_products?{0}".format(urlencode(params), server_url)
                  yield LinkDef(descriptor.pubDID, url, contentType="image/fits", description="SoFiA-2 Detection Moment2", semantics="#preview")

                  params = {"id": descriptor.pubDID, "product": "mask"}
                  url = "{1}/sofiax_detections/detection_products?{0}".format(urlencode(params), server_url)
                  yield LinkDef(descriptor.pubDID, url, contentType="image/fits", description="SoFiA-2 Detection Mask", semantics="#auxiliary")

                  params = {"id": descriptor.pubDID, "product": "channels"}
                  url = "{1}/sofiax_detections/detection_products?{0}".format(urlencode(params), server_url)
                  yield LinkDef(descriptor.pubDID, url, contentType="image/fits", description="SoFiA-2 Detection Channels", semantics="#auxiliary")

                  params = {"id": descriptor.pubDID, "product": "spectrum"}
                  url = "{1}/sofiax_detections/detection_products?{0}".format(urlencode(params), server_url)
                  yield LinkDef(descriptor.pubDID, url, contentType="text/plain", description="SoFiA-2 Detection Spectrum", semantics="#auxiliary")

                  url = "{1}/sofiax_detections/detection_products?id={0}".format(descriptor.pubDID, server_url)
                  yield LinkDef(descriptor.pubDID, url, contentType="application/x-tar", description="SoFiA-2 Detection Products", semantics="#this")
              </code>
           </metaMaker>

            <dataFunction>
               <setup>
                  <code>
                     from gavo.svcs import WebRedirect
                  </code>
               </setup>
               <code>
                  import os
                  server_url = os.environ.get('PRODUCT_URL', "http://localhost:8080")
                  url = "{1}/sofiax_detections/detection_products?id={0}".format(descriptor.pubDID, server_url)
                  raise WebRedirect(url)
               </code>
            </dataFunction>

		</datalinkCore>

	</service>


   <data id="import">
      <make table="run"/>
      <make table="instance"/>
      <make table="detection">
         <rowmaker>
            <map key="datalink_url">\dlMetaURI{dl}</map>
         </rowmaker>
      </make>
   </data>
</resource>